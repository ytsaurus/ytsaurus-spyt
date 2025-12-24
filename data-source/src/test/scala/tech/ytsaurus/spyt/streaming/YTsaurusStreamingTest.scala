package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.execution.StreamingUtils.{ROW_INDEX_WITH_PREFIX, TABLET_INDEX_WITH_PREFIX}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.InconsistentReadEnabled
import tech.ytsaurus.spyt.serializers.WriteSchemaConverter
import tech.ytsaurus.spyt.streaming.YTsaurusStreamingTest.{schemaWithServiceColumns, schemaWithServiceColumnsAndQueuePath}
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.TiType

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps

class YTsaurusStreamingTest extends AnyFlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils
  with TmpDir with DynTableTestUtils with QueueTestUtils {

  import spark.implicits._
  import tech.ytsaurus.spyt._

  it should "work with native key-value storage and FileContext YTsaurus API" in {
    YtWrapper.createDir(tmpPath)

    val numbers = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .select($"timestamp", ($"value" % 10).as("num"))

    val groupedNumbers = numbers
      .withWatermark("timestamp", "3 seconds")
      .groupBy(window($"timestamp", "1 seconds"), $"num")
      .count()

    val query = groupedNumbers
      .writeStream
      .option("checkpointLocation", s"yt:/$tmpPath/stateStore")
      .foreachBatch { (frame: Dataset[Row], _: Long) =>
        frame.write.mode(SaveMode.Append).yt(s"$tmpPath/result")
      }
      .start()

    var attempts = 180
    while (!yt.existsNode(s"$tmpPath/result").join() || (spark.read.yt(s"$tmpPath/result").count() == 0 && attempts > 0)) {
      Thread.sleep(10000)
      attempts -= 1
    }
    query.stop()

    val resultDF = spark.read.yt(s"$tmpPath/result")
    val receivedNums = resultDF.select(sum("count").cast("long")).first().getLong(0).toInt
    receivedNums should be > 0
  }

  it should "write YT queue" in {
    val recordCountLimit = 50L

    YtWrapper.createDir(tmpPath)
    val path = s"$tmpPath/result-${UUID.randomUUID()}"

    val numbers = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .select(floor(rand() * 10).as("num"))

    prepareOrderedTestTable(path, new WriteSchemaConverter().tableSchema(numbers.schema), enableDynamicStoreRead = true)

    val job = numbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .format("yt")
      .option("path", path)

    val recordFuture = Future[Unit] {
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        Thread.sleep(1000)
        currentCount = spark.read.option(InconsistentReadEnabled.name, "true").yt(path).count()
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    val query = job.start()
    Await.result(recordFuture, 150 seconds)
    query.stop()
  }

  it should "read YT queue" in {
    val recordCountLimit = 50L
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true)
    prepareConsumer(consumerPath, queuePath)
    waitQueueRegistration(queuePath)

    val numbers = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .load(queuePath)

    val stopSignal = Promise[Unit]()
    var recordCount = 0L

    val job = numbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .foreachBatch { (frame: DataFrame, batchNum: Long) =>
        recordCount += frame.count()
        if (recordCount >= recordCountLimit && !stopSignal.isCompleted) stopSignal.success()
        ()
      }

    val recordFuture = Future[Unit] {
      while (!stopSignal.isCompleted) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(2000)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    val query = job.start()
    Await.result(recordFuture, 120 seconds)
    query.stop()
  }

  it should "run pipeline on YT queues" in {
    val recordCountLimit = 50L
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"
    val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true)
    prepareConsumer(consumerPath, queuePath)
    waitQueueRegistration(queuePath)

    val numbers = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .load(queuePath)

    prepareOrderedTestTable(resultPath, new WriteSchemaConverter().tableSchema(numbers.schema), enableDynamicStoreRead = true)

    val job = numbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .format("yt")
      .option("path", resultPath)

    val recordFuture = Future[Unit] {
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(1000)
        currentCount = spark.read.option(InconsistentReadEnabled.name, "true").yt(resultPath).count()
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    val query = job.start()
    Await.result(recordFuture, 120 seconds)
    query.stop()
  }

  it should "correct streaming with max_rows_per_partition" in {
    val maxRowsPerPartition = 8
    val recordCountLimit = 50L
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"
    val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true)
    prepareConsumer(consumerPath, queuePath)
    waitQueueRegistration(queuePath)

    val resultSchema = getTestData().toDF().schema
    prepareOrderedTestTable(resultPath, new WriteSchemaConverter().tableSchema(resultSchema),
      enableDynamicStoreRead = true)

    val df: DataFrame = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .option("max_rows_per_partition", maxRowsPerPartition)
      .load(queuePath)

    val stopSignal = Promise[Unit]()
    var recordCount = 0L

    val jobForOptionCheck = df
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore_0")
      .foreachBatch { (frame: DataFrame, batchNum: Long) =>
        frame.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          assert(partition.size <= maxRowsPerPartition)
          ()
        }
        recordCount += frame.count()
        if (recordCount >= recordCountLimit && !stopSignal.isCompleted) stopSignal.success()
        ()
      }

    val recordFuture = Future[Unit] {
      while (!stopSignal.isCompleted) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(2000)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)


    val queryForOptionCheck = jobForOptionCheck.start()
    Await.result(recordFuture, 120 seconds)
    queryForOptionCheck.stop()
  }

  it should "one streaming launch" in {
    val launchesParams: Seq[(Int, Int, Boolean)] = Seq((0, 3, true))

    val paths: StreamingObjectsPaths = prepareStreamingObjects(tmpPath = tmpPath)
    doStreamLaunches(paths, launchesParams)

    val expectedData: Seq[TestRow] = getTestData(0, 29)
    val expectedDF: DataFrame = spark.createDataFrame(expectedData)

    val resultDF = spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(paths.resultPath)
      .orderBy("a")

    resultDF.dropDuplicates().collect() should contain theSameElementsAs expectedDF.collect()

  }

  it should "several streaming launches - at-least-once guarantee" in {
    testSeveralStreamingLaunches()
  }

  it should "several streaming launches - exactly-once guarantee" in {
    testSeveralStreamingLaunches(includeServiceColumns = true)
  }

  it should "fetch checkpoints location from consumer - at-least-once guarantee" in {
    testContinueStreamingAfterRemovingCheckpoints()
  }

  it should "fetch checkpoints location from consumer - exactly-once guarantee" in {
    testContinueStreamingAfterRemovingCheckpoints(includeServiceColumns = true)
  }

  it should "streaming with several sources-queues: check consumer and result synchronization" in {
    val queues = (0 until 3).map(i => s"$tmpPath/inputQueue$i-${UUID.randomUUID()}")
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"
    val checkpointLocation = f"yt:/$tmpPath/stateStore-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    for (queue <- queues) {
      prepareOrderedTestTable(queue, enableDynamicStoreRead = true)
      prepareConsumer(consumerPath, queue)
      waitQueueRegistration(queue)
    }
    val resultTableSchema: TableSchema = schemaWithServiceColumnsAndQueuePath
    prepareOrderedTestTable(resultPath, resultTableSchema, enableDynamicStoreRead = true)

    val queries = queues.map { queue =>
      spark
        .readStream
        .format("yt")
        .option("consumer_path", consumerPath)
        .option("max_rows_per_partition", 6)
        .option("include_service_columns", value = true)
        .load(queue)
        .withColumn("queue", lit(queue))
        .writeStream
        .option("checkpointLocation", checkpointLocation + "/" + queue)
        .format("yt")
        .option("path", resultPath)
        .start()
    }

    val queueWritingDuration = 10.seconds
    var endTime = System.currentTimeMillis() + queueWritingDuration.toMillis
    var lowerIndex = 0
    while (System.currentTimeMillis() < endTime) {
      queues.foreach { queue =>
        val data = getTestData(lowerIndex, lowerIndex + 9)
        appendChunksToTestTable(queue, Seq(data), sorted = false, remount = false)
      }
      lowerIndex += 10
      Thread.sleep(1000)
    }

    endTime = System.currentTimeMillis() + 30.seconds.toMillis
    queries.foreach(_.stop())
    while (queries.exists(_.isActive) && System.currentTimeMillis() < endTime) {
      Thread.sleep(1000)
    }

    val resultDF = spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(resultPath)
      .orderBy("a")
    val cluster = YtWrapper.clusterName()

    for (queue <- queues) {
      val currentOffsetPartitions = YtQueueOffset.getCurrentOffset(cluster, consumerPath, queue).partitions
      var totalRowsFromQueue = 0
      for (partitionIndex <- currentOffsetPartitions.keys) {
        val partitionDataDf = resultDF
          .filter(col("queue") === queue)
          .filter(col("__spyt_streaming_src_tablet_index") === partitionIndex)

        val totalRowsConsumed = currentOffsetPartitions(partitionIndex) + 1
        val totalRowsFromPartition = partitionDataDf.count.toInt
        totalRowsFromQueue += totalRowsFromPartition

        assert(totalRowsFromPartition >= totalRowsConsumed)

        val seq = (0 until totalRowsFromPartition).toArray
        partitionDataDf.select(col(ROW_INDEX_WITH_PREFIX)).collect().map(_.getLong(0).toInt) should contain theSameElementsAs seq
      }
    }
  }

  it should "streaming with several sources-queues: check full data delivery" in {
    val queues = (0 until 3).map(i => s"$tmpPath/inputQueue$i-${UUID.randomUUID()}")
    val iterarionsCount = Seq(1, 3, 5)
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"
    val checkpointLocation = f"yt:/$tmpPath/stateStore-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    for (queue <- queues) {
      prepareOrderedTestTable(queue, enableDynamicStoreRead = true)
      prepareConsumer(consumerPath, queue)
      waitQueueRegistration(queue)
    }
    val resultTableSchema: TableSchema = schemaWithServiceColumnsAndQueuePath
    prepareOrderedTestTable(resultPath, resultTableSchema, enableDynamicStoreRead = true)

    val queriesAndFutures = queues.zipWithIndex.map { case (queue, index) =>
      val query = spark
        .readStream
        .format("yt")
        .option("consumer_path", consumerPath)
        .option("max_rows_per_partition", 6)
        .option("include_service_columns", value = true)
        .load(queue)
        .withColumn("queue", lit(queue))
        .writeStream
        .option("checkpointLocation", checkpointLocation + "/" + queue)
        .format("yt")
        .option("path", resultPath)
        .start()

      val recordFuture = Future[Unit] {
        var lowerIndex = 0
        for (_ <- 0 until iterarionsCount(index)) {
          val data = getTestData(lowerIndex, lowerIndex + 9)
          appendChunksToTestTable(queue, Seq(data), sorted = false, remount = false)
          lowerIndex += 10
        }

        val recordCountLimit = iterarionsCount(index) * 10
        var currentCount = 0L
        while (currentCount < recordCountLimit) {
          Thread.sleep(2000)
          currentCount = spark.read.option(InconsistentReadEnabled.name, "true").yt(resultPath).filter(col("queue") === queue).count()
        }

      }(scala.concurrent.ExecutionContext.Implicits.global)

      (query, recordFuture)
    }

    val allFutures = queriesAndFutures.map(_._2)
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(Future.sequence(allFutures), 120.seconds)
    queriesAndFutures.foreach { case (query, _) => query.stop() }

    val resultDF = spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(resultPath)
      .orderBy("a")

    queues.zipWithIndex.map { case (queue, index) =>
      val countElems = iterarionsCount(index) * 10
      val expectedDF: DataFrame = spark.createDataFrame(getTestData(0, countElems - 1))
      val filteredDf = resultDF.filter(col("queue") === queue).select("a", "b", "c")
      filteredDf.collect() should contain theSameElementsAs expectedDF.collect()
    }
  }

  it should "streaming with several sinks and consumers" in {
    val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"
    val checkpointLocation = f"yt:/$tmpPath/stateStore-${UUID.randomUUID()}"
    val resultTableSchema: TableSchema = schemaWithServiceColumnsAndQueuePath
    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true)

    val consumersAndResultPathsPairs: Seq[(String, String)] = (1 to 3).map { i =>
      val consumerPath = s"$tmpPath/consumer$i-${UUID.randomUUID()}"
      prepareConsumer(consumerPath, queuePath)
      val resultPath = s"$tmpPath/result$i-${UUID.randomUUID()}"
      prepareOrderedTestTable(resultPath, resultTableSchema, enableDynamicStoreRead = true)
      (consumerPath, resultPath)
    }
    waitQueueRegistration(queuePath)

    val queries = consumersAndResultPathsPairs.map { pair =>
      spark
        .readStream
        .format("yt")
        .option("consumer_path", pair._1)
        .option("max_rows_per_partition", 6)
        .option("include_service_columns", value = true)
        .load(queuePath)
        .withColumn("queue", lit(queuePath))
        .writeStream
        .option("checkpointLocation", checkpointLocation + "/" + pair._1)
        .format("yt")
        .option("path", pair._2)
        .start()
    }

    val iterations = 3
    var lowerIndex = 0
    for (_ <- 0 until iterations) {
      val data = getTestData(lowerIndex, lowerIndex + 9)
      appendChunksToTestTable(queuePath, Seq(data), sorted = false, remount = false)
      lowerIndex += 10
    }

    val recordCountLimit = iterations * 10
    val waitingTimeout = 120.seconds
    val endTime = System.currentTimeMillis() + waitingTimeout.toMillis
    while (System.currentTimeMillis() < endTime &&
      !consumersAndResultPathsPairs.map(pair => pair._2).forall(resultPath =>
        spark.read.option(InconsistentReadEnabled.name, "true").yt(resultPath).count() >= recordCountLimit
      )) {
      Thread.sleep(2000)
    }
    queries.foreach(_.stop)

    for (resultPath <- consumersAndResultPathsPairs.map(pair => pair._2)) {
      val resultDF = spark.read
        .option(InconsistentReadEnabled.name, "true")
        .yt(resultPath)
        .orderBy("a")
        .select("a", "b", "c")

      val expectedDF: DataFrame = spark.createDataFrame(getTestData(0, 29))
      resultDF.collect() should contain theSameElementsAs expectedDF.collect()
    }
  }

  it should "streaming after offsets desynchronization: checkpoints are behind on consumer" in {
    val paths: StreamingObjectsPaths = prepareStreamingObjects(tmpPath = tmpPath, includeServiceColumns = true,
      queueTabletCount = 1)
    val consumerPath = paths.consumerPath
    val queuePath = paths.queuePath
    val resultPath = paths.resultPath
    val checkpointLocation = paths.checkpointLocation

    doStreamLaunches(paths, launchesParams = Seq((0, 3, true)), includeServiceColumns = true)

    val cluster = YtWrapper.clusterName()
    val currentOffset = YtQueueOffset.getCurrentOffset(cluster, consumerPath, queuePath)
    // Advance consumer to simulate the desynchronization of the consumer and cached offsets by spark / checkpoint files
    val shiftedPartitions = currentOffset.partitions.map { case (partitionId, _) => partitionId -> 34L }
    val shiftedOffset = YtQueueOffset(cluster, queuePath, shiftedPartitions)
    YtQueueOffset.advance(consumerPath, shiftedOffset, currentOffset)

    val df: DataFrame = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .option("include_service_columns", value = true)
      .load(queuePath)

    val queryForResultTableCheck = df
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .format("yt")
      .option("path", resultPath)
      .start()

    val recordFuture = Future[Unit] {
      var lowerIndex = 30
      for (_ <- 0 until 3) {
        val data = getTestData(lowerIndex, lowerIndex + 9)
        appendChunksToTestTable(queuePath, Seq(data), sorted = false, remount = false)
        lowerIndex += 10
      }

      val recordCountLimit = 55 // expect spark to skip 5 lines, because we have shifted the offset in consumer
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        Thread.sleep(2000)
        currentCount = spark.read.option(InconsistentReadEnabled.name, "true").yt(resultPath).count()
      }

    }(scala.concurrent.ExecutionContext.Implicits.global)

    Await.result(recordFuture, 120 seconds)
    queryForResultTableCheck.stop()
  }

  it should "correctly stream unsigned byte, short and int types" in {
    case class UnsignedTestRow(uint8: Short, uint16: Int, uint32: Long)

    val unsignedSchema: TableSchema = TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("uint8", TiType.optional(TiType.uint8()))
      .addValue("uint16", TiType.uint16())
      .addValue("uint32", TiType.uint32())
      .build()

    val testData = Seq(
      UnsignedTestRow(0, 0, 0L),
      UnsignedTestRow(255, 65535, 4294967295L),
      UnsignedTestRow(128, 32768, 2147483648L),
      UnsignedTestRow(1, 1, 1L)
    )

    val paths = new StreamingObjectsPaths(tmpPath)
    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(paths.queuePath, unsignedSchema, tabletCount = 1, enableDynamicStoreRead = true)
    prepareConsumer(paths.consumerPath, paths.queuePath)
    prepareOrderedTestTable(paths.resultPath, unsignedSchema, tabletCount = 1, enableDynamicStoreRead = true)
    waitQueueRegistration(paths.queuePath)

    appendChunksToTestTable(path = paths.queuePath, schema = unsignedSchema, data = Seq(testData), sorted = false,
      remount = false)

    val query = spark.readStream
      .format("yt")
      .option("consumer_path", paths.consumerPath)
      .option("parsing_type_v3", "true")
      .load(paths.queuePath)
      .writeStream
      .option("checkpointLocation", paths.checkpointLocation)
      .format("yt")
      .option("path", paths.resultPath)
      .start()

    var resultCount = 0L
    val startTime = System.currentTimeMillis()
    while (resultCount < testData.size && (System.currentTimeMillis() - startTime) < 20 * 1000) {
      Thread.sleep(1000)
      resultCount = spark.read.option(InconsistentReadEnabled.name, "true").yt(paths.resultPath).count()
    }
    query.stop()

    val resultDF = spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(paths.resultPath)
      .select("uint8", "uint16", "uint32")

    resultDF.schema.fields.map(_.copy(metadata = Metadata.empty)) shouldBe StructType(Seq(
      StructField("uint8", ShortType, nullable = true),
      StructField("uint16", IntegerType, nullable = true),
      StructField("uint32", LongType, nullable = true)
    ))

    val actualData = resultDF.collect().map(row =>
      Seq(row.getAs[Short]("uint8"), row.getAs[Int]("uint16"), row.getAs[Long]("uint32"))
    )

    val expectedData = testData.map(row => Seq(row.uint8, row.uint16, row.uint32))

    actualData should contain theSameElementsAs expectedData
  }

  def testContinueStreamingAfterRemovingCheckpoints(includeServiceColumns: Boolean = false): Unit = {
    val paths: StreamingObjectsPaths = prepareStreamingObjects(tmpPath = tmpPath, includeServiceColumns)
    doStreamLaunches(paths, launchesParams = Seq((0, 3, true)), includeServiceColumns)

    YtWrapper.removeDir(paths.checkpointLocation.stripPrefix("yt:/"), recursive = true, force = true)

    doStreamLaunches(paths, launchesParams = Seq((30, 3, true)), includeServiceColumns)

    val expectedData: Seq[TestRow] = getTestData(0, 59)
    val expectedDF: DataFrame = spark.createDataFrame(expectedData)

    val resultDF = spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(paths.resultPath)
      .select("a", "b", "c")
      .orderBy("a")

    if (includeServiceColumns) {
      resultDF.collect() should contain theSameElementsAs expectedDF.collect()
    } else {
      resultDF.dropDuplicates().collect() should contain theSameElementsAs expectedDF.collect()
    }
  }

  def testSeveralStreamingLaunches(includeServiceColumns: Boolean = false): Unit = {
    val paths: StreamingObjectsPaths = prepareStreamingObjects(tmpPath = tmpPath, includeServiceColumns)
    val launchesParams: Seq[(Int, Int, Boolean)] = Seq(
      (0, 3, false),
      (30, 3, false),
      (60, 3, true)
    )
    doStreamLaunches(paths, launchesParams, includeServiceColumns)

    val resultDF = spark.read
      .option(InconsistentReadEnabled.name, "true")
      .yt(paths.resultPath)
      .select("a", "b", "c")
      .orderBy("a")

    val expectedData: Seq[TestRow] = getTestData(0, 89)
    val expectedDF: DataFrame = spark.createDataFrame(expectedData)

    if (includeServiceColumns) {
      resultDF.collect() should contain theSameElementsAs expectedDF.collect()
    } else {
      resultDF.dropDuplicates().collect() should contain theSameElementsAs expectedDF.collect()
    }
  }

  def prepareStreamingObjects(tmpPath: String, includeServiceColumns: Boolean = false, queueTabletCount: Int = 3): StreamingObjectsPaths = {
    val paths: StreamingObjectsPaths = new StreamingObjectsPaths(tmpPath)
    val consumerPath = paths.consumerPath
    val queuePath = paths.queuePath
    val resultPath = paths.resultPath

    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true, tabletCount = queueTabletCount)
    prepareConsumer(consumerPath, queuePath)

    val resultTableSchema = if (includeServiceColumns) schemaWithServiceColumns else orderedTestSchema
    prepareOrderedTestTable(resultPath, resultTableSchema, enableDynamicStoreRead = true)

    waitQueueRegistration(queuePath)

    paths
  }

  def doStreamLaunches(paths: StreamingObjectsPaths, launchesParams: Seq[(Int, Int, Boolean)], includeServiceColumns: Boolean = false): Unit = {
    val consumerPath = paths.consumerPath
    val queuePath = paths.queuePath
    val resultPath = paths.resultPath
    val checkpointLocation = paths.checkpointLocation

    val df: DataFrame = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .option("include_service_columns", includeServiceColumns)
      .load(queuePath)

    def runStreamUntilAchieveRecordCountLimit(startIndex: Int, iterations: Int,
                                              readUntilFullDataDelivery: Boolean = false): Unit = {
      val queryForResultTableCheck = df
        .writeStream
        .option("checkpointLocation", checkpointLocation)
        .format("yt")
        .option("path", resultPath)
        .start()

      val recordFuture = Future[Unit] {
        var lowerIndex = startIndex
        for (_ <- 0 until iterations) {
          val data = getTestData(lowerIndex, lowerIndex + 9)
          appendChunksToTestTable(queuePath, Seq(data), sorted = false, remount = false)
          lowerIndex += 10
        }

        if (readUntilFullDataDelivery) {
          val recordCountLimit = startIndex + iterations * 10
          var currentCount = 0L

          while (currentCount < recordCountLimit) {
            Thread.sleep(2000)
            currentCount = spark.read.option(InconsistentReadEnabled.name, "true").yt(resultPath).dropDuplicates().count()
          }
        }
      }(scala.concurrent.ExecutionContext.Implicits.global)

      Await.result(recordFuture, 120 seconds)
      queryForResultTableCheck.stop()
    }

    for (launchParams <- launchesParams) {
      runStreamUntilAchieveRecordCountLimit(launchParams._1, launchParams._2, launchParams._3)
    }
  }
}

object YTsaurusStreamingTest {

  val PROCESSING_TIME_TRIGGER: Trigger = ProcessingTime(1000)

  val schemaWithServiceColumns: TableSchema = {
    var builder: TableSchema.Builder = TableSchema.builder().setUniqueKeys(false)
    builder = addServiceColumnsAsKeys(builder)
    builder = addValueColumns(builder)
    builder.build()
  }

  val schemaWithServiceColumnsAndQueuePath: TableSchema = {
    var builder: TableSchema.Builder = TableSchema.builder().setUniqueKeys(false)
    builder = builder.addKey(s"queue", ColumnValueType.STRING)
    builder = addServiceColumnsAsKeys(builder)
    builder = addValueColumns(builder)
    builder.build()
  }

  def addValueColumns(builder: TableSchema.Builder): TableSchema.Builder = {
    builder
      .addValue("a", ColumnValueType.INT64)
      .addValue("b", ColumnValueType.INT64)
      .addValue("c", ColumnValueType.STRING)
  }

  def addServiceColumnsAsKeys(builder: TableSchema.Builder): TableSchema.Builder = {
    builder
      .addKey(TABLET_INDEX_WITH_PREFIX, ColumnValueType.INT64)
      .addKey(ROW_INDEX_WITH_PREFIX, ColumnValueType.INT64)
  }

}

class StreamingObjectsPaths(tmpPath: String) {
  val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
  val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"
  val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"
  val checkpointLocation = f"yt:/$tmpPath/stateStore-${UUID.randomUUID()}"
}
