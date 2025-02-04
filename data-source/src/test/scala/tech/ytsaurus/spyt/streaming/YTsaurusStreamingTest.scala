package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.serializers.WriteSchemaConverter
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps

class YTsaurusStreamingTest extends AnyFlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils
  with TmpDir with DynTableTestUtils with QueueTestUtils {

  import spark.implicits._
  import tech.ytsaurus.spyt._

  it should "work with native key-value storage and FileContext YTsaurus API" in {
    val batchCount = 3L
    val batchSeconds = 5

    YtWrapper.createDir(tmpPath)

    val numbers = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .select($"timestamp", floor(rand() * 10).as("num"))

    val stopSignal = Promise[Unit]()

    val groupedNumbers = numbers
      .withWatermark("timestamp", "5 seconds")
      .groupBy(window($"timestamp", "5 seconds", "3 seconds"), $"num")
      .count()

    val job = groupedNumbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .trigger(ProcessingTime(batchSeconds * 1000))
      .foreachBatch { (frame: DataFrame, batchNum: Long) =>
        if (batchNum >= batchCount) {
          if (!stopSignal.isCompleted) stopSignal.success()
          ()
        } else {
          frame.write.mode(SaveMode.Append).yt(s"$tmpPath/result")
        }
      }

    val query = job.start()
    Await.result(stopSignal.future, 420 seconds)
    query.stop()

    val resultDF = spark.read.yt(s"$tmpPath/result")
    val receivedNums = resultDF.select(sum("count").cast("long")).first().getLong(0)
    receivedNums should be >= ((batchCount - 1) * batchSeconds)
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
      .trigger(ProcessingTime(2000))
      .format("yt")
      .option("path", path)

    val recordFuture = Future[Unit] {
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        Thread.sleep(1000)
        currentCount = spark.read.option("enable_inconsistent_read", "true").yt(path).count()
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
      .trigger(ProcessingTime(2000))
      .foreachBatch { (frame: DataFrame, batchNum: Long) =>
        recordCount += frame.count()
        if (recordCount >= recordCountLimit && !stopSignal.isCompleted) stopSignal.success()
        ()
      }

    val recordFuture = Future[Unit] {
      while (!stopSignal.isCompleted) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(4000)
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
      .trigger(ProcessingTime(2000))
      .format("yt")
      .option("path", resultPath)

    val recordFuture = Future[Unit] {
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(2000)
        currentCount = spark.read.option("enable_inconsistent_read", "true").yt(resultPath).count()
        print(currentCount)
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
      .trigger(ProcessingTime(2000))
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
        Thread.sleep(4000)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)


    val queryForOptionCheck = jobForOptionCheck.start()
    Await.result(recordFuture, 120 seconds)
    queryForOptionCheck.stop()
  }

  it should "correct results using streaming" in {
    val maxRowsPerPartition = 6
    val recordCountLimit = 30L
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

    val queryForResultTableCheck = df
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore_1")
      .trigger(ProcessingTime(2000))
      .format("yt")
      .option("path", resultPath)
      .start()

    val recordFuture = Future[Unit] {
      var lowerIndex = 0
      for (_ <- 0 to 2) {
        val data = getTestData(lowerIndex, lowerIndex + 9)
        appendChunksToTestTable(queuePath, Seq(data), sorted = false, remount = false)
        lowerIndex += 10
      }

      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        Thread.sleep(3000)
        currentCount = spark.read.option("enable_inconsistent_read", "true").yt(resultPath).count()
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    Await.result(recordFuture, 120 seconds)
    queryForResultTableCheck.stop()

    val expectedData: Seq[TestRow] = getTestData(0, recordCountLimit.toInt - 1)
    val expectedDF: DataFrame = spark.createDataFrame(expectedData)

    val resultDF = spark.read
      .option("enable_inconsistent_read", "true")
      .yt(resultPath)
      .orderBy("a")

    resultDF.collect() should contain theSameElementsAs expectedDF.collect()

    val duplicatesCount = resultDF
      .groupBy("a", "b", "c")
      .count()
      .filter("count > 1")
      .count()
    assert(duplicatesCount == 0, s"Found $duplicatesCount duplicate rows in the result set.")
  }
}