package tech.ytsaurus.spyt.format

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtWriter
import tech.ytsaurus.spyt.format.YtDistributedWriterTest.{SampleRow, extractId}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.ysontree.YTreeNode

import scala.util.Random

class YtDistributedWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils {
  behavior of "Spark"

  override def numFailures: Int = 4

  override def reinstantiateSparkSession: Boolean = true

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.yt.write.distributed.enabled", "true")
  }

  private def baseTest(
    _spark: SparkSession,
    transformData: Seq[SampleRow] => Seq[SampleRow] = identity,
    transformDs: Dataset[SampleRow] => Dataset[SampleRow] = identity,
    testSorting: Boolean = false
  ): Unit = {
    import _spark.implicits._
    val data = (1 to 100).map(id => SampleRow(id, s"Value of $id"))
    val df = transformDs(_spark.createDataset(transformData(data)).repartition(10))
    var dfWriter = df.write
    if (testSorting) {
      dfWriter = dfWriter.sortedBy("id")
    }
    dfWriter.yt(tmpPath)

    val writtenData = readTableAsYson(tmpPath)
    val actual = writtenData.map(node => SampleRow(
      node.asMap().get("id").longValue(),
      node.asMap().get("value").stringValue()
    ))

    actual should have size data.size
    if (testSorting) {
      actual should contain theSameElementsInOrderAs data
    } else {
      actual should contain theSameElementsAs data
    }
  }

  it should "write data to yt using distributed writer" in withSparkSession() { _spark =>
    baseTest(_spark)
  }

  it should "deal with failed tasks before or after commit" in withSparkSession(Map(
    "spark.sql.sources.commitProtocolClass" -> "tech.ytsaurus.spyt.format.BogusYtOutputCommitProtocol"
  )) { _spark =>
    baseTest(_spark)
  }

  it should "correctly stop broadcasting cookies in case of job fail after commit" in withSparkSession(Map(
    "spark.sql.sources.commitProtocolClass" -> "tech.ytsaurus.spyt.format.BogusYtOutputJobCommitProtocol"
  )) { _spark =>
    var exception: Throwable = intercept[Exception](baseTest(_spark))
    if (exception.isInstanceOf[SparkException]) {
      exception.getCause shouldNot be (null)
      exception.getCause shouldBe a[RuntimeException]
      exception = exception.getCause
    }
    exception shouldBe a[RuntimeException]
    exception.getMessage shouldEqual "BOOOOM!!!!"
  }

  it should "do a simple write" in withSparkSession() { _spark =>
    _spark.range(1, 10).write.yt(s"$tmpPath/table3")
    readTableAsYson(s"$tmpPath/table3").map(extractId) should contain theSameElementsAs (1 until 10)
  }

  it should "deal with two simultaneous writes" in withSparkSession() { _spark =>
    YtWrapper.createDir(s"$tmpPath/parent1")
    YtWrapper.createDir(s"$tmpPath/parent2")

    val t1 = new Thread() {
      override def run(): Unit = {
        _spark.range(1, 10).write.yt(s"$tmpPath/parent1/table1")
      }
    }
    val t2 = new Thread() {
      override def run(): Unit = {
        _spark.range(1, 20).write.yt(s"$tmpPath/parent2/table2")
      }
    }
    t1.start()
    t2.start()

    t1.join()
    t2.join()

    readTableAsYson(s"$tmpPath/parent1/table1").map(extractId) should contain theSameElementsAs (1 until 10)
    readTableAsYson(s"$tmpPath/parent2/table2").map(extractId) should contain theSameElementsAs (1 until 20)
  }

  it should "write sorted data to sorted table" in withSparkSession() { _spark =>
    baseTest(
      _spark,
      data => Random.shuffle(data),
      ds => ds.orderBy("id"),
      testSorting = true
    )
  }

  it should "overwrite existing table" in withSparkSession() { _spark =>
    writeTableFromYson(Seq("{a = 1}", "{a = 2}"), tmpPath, TableSchema.builder()
        .addValue("a", ColumnValueType.INT64).build(),
    )

    _spark.range(100).write.mode(SaveMode.Overwrite).yt(tmpPath)

    readTableAsYson(tmpPath) should have size 100
  }

  it should "correctly work when AQE is enabled and SQL has subqueries" in withSparkSession() { _spark =>
    _spark.range(1, 2).createOrReplaceTempView("inner1")
    _spark.range(3, 4).createOrReplaceTempView("inner2")
    _spark.range(1, 10000).createOrReplaceTempView("main_table")

    val query = """
      select
        m.id as m_id,
        (select id from inner1) as id_1,
        (select id from inner2) as id_2
      from main_table m
      """

    val df = _spark.sql(query)
    df.write.yt(tmpPath)

    val writtenData = readTableAsYson(tmpPath).map { node =>
      val nodeMap = node.asMap()
      (
        nodeMap.get("m_id").longValue(),
        nodeMap.get("id_1").longValue(),
        nodeMap.get("id_2").longValue()
      )
    }
    val expectedData = (1 to 9999).map(id => (id.longValue(), 1L, 3L))
    writtenData should contain theSameElementsAs expectedData
  }
}

object YtDistributedWriterTest {
  case class SampleRow(id: Long, value: String)

  def extractId(node: YTreeNode): Long = node.asMap().get("id").longValue()
}

