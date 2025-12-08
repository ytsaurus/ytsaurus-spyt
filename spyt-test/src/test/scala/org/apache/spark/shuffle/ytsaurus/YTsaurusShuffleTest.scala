package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.shuffle.CommitShufflePartitionsListener
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{SpytRpcClientListener, YtClientConfiguration, YtClientProvider}

import scala.sys.process._
import scala.util.{Failure, Random, Success, Try}

class YTsaurusShuffleTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with TableDrivenPropertyChecks {
  import YTsaurusShuffleTest._

  behavior of "YTsaurus shuffle service"

  private val schema = TableSchema.builder()
    .addValue("id", ColumnValueType.INT64)
    .addValue("value", ColumnValueType.INT64)
    .build()

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager")
      .set("spark.shuffle.sort.io.plugin.class", "tech.ytsaurus.spyt.shuffle.YTsaurusShuffleDataIO")

      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "9")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .set("spark.shuffle.readHostLocalDisk", "false")
      .set("spark.task.maxFailures", "100") // To allow retries on tests with failures so Spark job won't be aborted

      //For skewed partitions test
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1k")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1k")

      .set("spark.ytsaurus.shuffle.write.row.size", "16k")
      .set("spark.ytsaurus.shuffle.replication.factor", "1")
  }

  override def sparkMaster: String = "local-cluster[3, 4, 1024]"

  override def reinstantiateSparkSession: Boolean = true

  private def testName(numRows: Int, failAtProcess: Boolean, failAfterCommit: Boolean): String =
    s"be used to write shuffle data and to read shuffle data when one of the executors is lost " +
      s"for $numRows number of rows, Fail at process: $failAtProcess, Fail after commit: $failAfterCommit"

  private def readSourceData(_spark: SparkSession, path: String, failAtProcess: Boolean): Dataset[SourceRow] = {
    import _spark.implicits._
    _spark.read.yt(path).repartition(24).as[SourceRow].mapPartitions { iterator => new Iterator[SourceRow] {

      override def hasNext: Boolean = {
        if (!iterator.hasNext && failAtProcess && pseudoRandom.nextInt(10) == 0) {
          // Exploding at the end of a partition, thus writing some data to it, then failing a task and causing
          // to reprocess this data again
          throw new RuntimeException("BOOOM")
        } else {
          iterator.hasNext
        }
      }

      override def next(): SourceRow = iterator.next()
    }}
  }

  private def createShuffledTable(path: String, numRows: Int, valueGen: Int => Int): Unit = {
    val rows = (1 to numRows).map(n => s"""{id = $n; value = ${s"value ${valueGen(n)}".hashCode}}""")
    writeTableFromYson(pseudoRandom.shuffle(rows), path, schema)
  }

  private def testTemplate(
    numRows: Int,
    failAtProcess: Boolean,
    failAfterCommit: Boolean,
    useCompression: Boolean
  ): Unit = it should testName(numRows, failAtProcess, failAfterCommit) in withSparkSession(
    Map("spark.shuffle.compress" -> useCompression.toString) ++
      (if (failAfterCommit) Map("spark.ytsaurus.test.failAfterCommit" -> "true") else Map.empty)
  ) { _spark =>
    import _spark.implicits._
    val numRowsX2 = numRows * 2

    YtWrapper.createDir(s"$tmpPath")
    val inPathFirst = s"$tmpPath/first"
    val inPathSecond = s"$tmpPath/second"

    createShuffledTable(inPathFirst, numRows, identity)
    createShuffledTable(inPathSecond, numRowsX2, x => x*x)

    // Since we're using here local-cluster Spark master hence Executors are child processes of the test process.
    // So here we are choosing one child executor process as a victim to kill it later.
    val executorProcessToKillOpt = ProcessHandle.current().descendants().filter { child =>
      Try(Seq("cat", s"/proc/${child.pid()}/cmdline").!!) match {
        case Success(procCmd) => procCmd.contains("CoarseGrainedExecutorBackend")
        case Failure(exception) =>
          println("Got an exception during getting child processes command")
          exception.printStackTrace(System.out)
          false
      }
    }.findAny()

    if (executorProcessToKillOpt.isEmpty) {
      fail("No executor child processes is found, there must be at least one")
    }
    val executorProcessToKill = executorProcessToKillOpt.get()
    val executorKillerListener = new ExecutorKillerSparkListener(executorProcessToKill)

    _spark.sparkContext.addSparkListener(executorKillerListener)

    val df1 = readSourceData(_spark, inPathFirst, failAtProcess)
    val df2 = readSourceData(_spark, inPathSecond, failAtProcess)

    val result = df2.join(df1, df2("id") === df1("id")*2, "left")
      .select(df2("id"), df2("value"), df1("id").as("joined_id"), df1("value").as("joined_value"))
    val resultData = result.as[JoinResult].collect()
    resultData.length shouldBe numRowsX2

    val sortedResultData = resultData.sortBy(_.id)
    val expectedResult = (1 to numRowsX2).map { n =>
      val joinedN = Some(n).filter(_ % 2 == 0).map(_.toLong / 2)
      JoinResult(n, s"value ${n*n}".hashCode, joinedN, joinedN.map(i => s"value $i".hashCode))
    }

    sortedResultData should contain theSameElementsInOrderAs expectedResult
  }

  private val testParameters = Table(
    ("Number of rows", "Fail at process", "Fail after commit", "Compression"),
    (100, false, false, false),
    (1000, true, false, false),
    (10000, false, true, false),
    (100000, false, false, true),
    (10000000, true, true, true)
  )
  forEvery(testParameters)(testTemplate)

  it should "sort a table using YTsaurus shuffle service" in withSparkSession() { _spark =>
    import _spark.implicits._
    // Create a large table with shuffled sort key
    val numRows = 300
    createShuffledTable(tmpPath, numRows, identity)

    // Repartition it so the dataframe will consist of more than one partition
    val df = _spark.read.yt(tmpPath)

    // Sort it using spark
    val result = df.repartition(3).sort($"id".asc).as[SourceRow].collect()

    // Check the result
    val expectedResult = (1 to numRows).map(id => SourceRow(id, s"value $id".hashCode))
    result should contain theSameElementsInOrderAs expectedResult
  }

  it should "correctly coalesce shuffle partitions" in withSparkSession(
    Map("spark.sql.adaptive.coalescePartitions.enabled" -> "true")
  ) { _spark =>
    import _spark.implicits._
    val nRows = 100
    val df1 = (1 to nRows).zip((1 to nRows).map(x => x * x)).toDF("key", "square")
    val df2 = (1 to nRows).zip((1 to nRows).map(x => x * x * x)).toDF("key", "cube")

    val joined = df1.join(df2, "key")

    val result = joined.collect()
    result.length shouldBe nRows
  }

  private def generateKeyValueDataframe(_spark: SparkSession, nRows: Int,
                                        percentOfOnes: Int, numPartitions: Option[Int] = None): DataFrame = {
    import _spark.implicits._
    val nOnes = nRows * percentOfOnes / 100
    val nRest = nRows - nOnes + 1
    val seq = (Seq.fill(nOnes)(1) ++ (2 to nRest)).zip(1 to nRows)
    val rdd = numPartitions match {
      case Some(nPartitions) => _spark.sparkContext.parallelize(seq, nPartitions)
      case None => _spark.sparkContext.parallelize(seq)
    }

    rdd.toDF("key", "value")
  }

  it should "correctly work with skewed partitions in join scenario" in withSparkSession() {_spark =>
    import _spark.implicits._
    val df1 = generateKeyValueDataframe(_spark, 1000, 95)
    val df2 = (1 to 10).zip(111 to 120).toDF("key", "value2")
    val joinedDf = df1.join(df2, Seq("key"), "left")

    val result = joinedDf.collect()

    result.length shouldBe 1000
  }

  it should "correctly work with skewed partitions in rebalance scenario" in withSparkSession(
    Map("spark.shuffle.compress" -> "false")
  ) { _spark =>
    import _spark.implicits._
    val nRows = 1000
    val df = generateKeyValueDataframe(_spark, nRows, 90, Some(200))

    df.repartition($"key").cache().createOrReplaceTempView("repartitioned")

    val result = _spark.sql("SELECT /*+ REBALANCE */ * FROM repartitioned")

    result.collect().length shouldBe nRows
  }

  it should "deal with exceptions thrown by createShuffleReader method" in withSparkSession(
    Map("spark.ytsaurus.client.provider.class" ->
      "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleTest$BogusYtClientProvider")
  ) { _spark =>
    import _spark.implicits._
    val df = (1 to 10000).toDF("id")
    val result = df.groupBy($"id" % 11).count().collect()

    result.length shouldBe 11
  }
}

object YTsaurusShuffleTest {
  // Random sequence should be determenistic in order to fail at predictable times
  val pseudoRandom = new Random(4097)

  case class SourceRow(id: Long, value: Long)

  case class JoinResult(id: Long, value: Long, joined_id: Option[Long], joined_value: Option[Long])

  class ExecutorKillerSparkListener(victimProcess: ProcessHandle) extends SparkListener {
    private var alreadyShot = false
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      val isLastStage = stageSubmitted.properties.getProperty("spark.rdd.scope", "").contains("\"name\":\"collect\"")

      if (isLastStage && !alreadyShot) {
        victimProcess.destroyForcibly()
        alreadyShot = true
      }
    }
  }

  class TestCommitShufflePartitionsListener extends CommitShufflePartitionsListener {

    override def onCommitPartitions(): Unit = {
      val failAfterCommit = SparkEnv.get.conf.getBoolean("spark.ytsaurus.test.failAfterCommit", defaultValue = false)
      if (failAfterCommit && pseudoRandom.nextInt(6) == 0) {
        throw new RuntimeException("BOOOM!!!")
      }
    }
  }

  class BogusYtClientProvider extends YtClientProvider {
    override def ytClient(conf: YtClientConfiguration,
                          rpcClientListener: Option[SpytRpcClientListener]): CompoundClient = {
      val realClient = YtClientProvider.ytClient(conf)
      val spyClient = Mockito.spy(realClient)

      Mockito.doAnswer { invocation =>
        if (pseudoRandom.nextInt(4) == 0) {
          throw new RuntimeException("BOOOM!!!")
        }
        invocation.callRealMethod()
      }.when(spyClient).createShuffleReader(any())

      spyClient
    }
  }
}
