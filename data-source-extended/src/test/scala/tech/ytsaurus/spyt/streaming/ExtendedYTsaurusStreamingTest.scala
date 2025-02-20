package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.streaming.ExtendedYTsaurusStreamingTest.CustomTypesRowAsPrimitives
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.types.UInt64Long
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.TiType

import java.util.UUID
import scala.concurrent._
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps


class ExtendedYTsaurusStreamingTest extends AnyFlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils
  with TmpDir with DynTableTestUtils with QueueTestUtils {

  import tech.ytsaurus.spyt._

  // TODO: add yson and composite types after finish SPYT-782 (mihailagei)
  it should "correct streaming with custom YT types" in {
    withConf(SparkYtConfiguration.Schema.ForcingNullableIfNoMetadata, false) {
      val recordCountLimit = 10L
      val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
      val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"
      val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"

      val ytSchema: TableSchema = TableSchema.builder()
        .setUniqueKeys(false)
        .addValue("datetime", TiType.datetime())
        .addValue("date32", TiType.date32())
        .addValue("datetime64", TiType.datetime64())
        .addValue("timestamp64", TiType.timestamp64())
        .addValue("interval64", TiType.interval64())
        .addValue("uint64", TiType.uint64())
        .build()

      YtWrapper.createDir(tmpPath)
      prepareOrderedTestTable(queuePath, ytSchema, enableDynamicStoreRead = true)
      prepareConsumer(consumerPath, queuePath)
      waitQueueRegistration(queuePath)

      prepareOrderedTestTable(resultPath, ytSchema, enableDynamicStoreRead = true)

      val df: DataFrame = spark
        .readStream
        .format("yt")
        .option("consumer_path", consumerPath)
        .load(queuePath)

      val queryForResultTableCheck = df
        .writeStream
        .option("checkpointLocation", f"yt:/$tmpPath/checkpoints_${UUID.randomUUID()}")
        .trigger(ProcessingTime(2000))
        .format("yt")
        .option("path", resultPath)
        .start()

      val recordFuture = Future[Unit] {
        var lowerIndex = 0
        for (_ <- 0 to 4) {
          val data = (lowerIndex to lowerIndex + 1).map(i => CustomTypesRowAsPrimitives(
            i.toLong,
            i,
            i.toLong,
            i.toLong,
            i.toLong,
            i.toLong
          ))
          appendChunksToTestTable(queuePath, schema = ytSchema, data = Seq(data), sorted = true, remount = false)
          lowerIndex += 2
        }

        var currentCount = 0L
        while (currentCount < recordCountLimit) {
          Thread.sleep(3000)
          currentCount = spark.read.option("enable_inconsistent_read", "true").yt(resultPath).count()
        }
      }(scala.concurrent.ExecutionContext.Implicits.global)

      Await.result(recordFuture, 120 seconds)
      queryForResultTableCheck.stop()

      val resultDF = spark.read
        .option("enable_inconsistent_read", "true")
        .yt(resultPath)
        .orderBy("uint64")

      val expectedData: Seq[Row] = (0 until recordCountLimit.toInt).map(i => Row(
        Datetime(i.toLong),
        new Date32(i),
        new Datetime64(i.toLong),
        new Timestamp64(i.toLong),
        new Interval64(i.toLong),
        UInt64Long(i.toLong)
      ))

      val sparkSchema: StructType = {
        StructType(Seq(
          StructField("datetime", new DatetimeType(), nullable = false),
          StructField("date32", new Date32Type(), nullable = false),
          StructField("datetime64", new Datetime64Type(), nullable = false),
          StructField("timestamp64", new Timestamp64Type(), nullable = false),
          StructField("interval64", new Interval64Type(), nullable = false),
          StructField("uint64", UInt64Type, nullable = false),
        ))
      }

      val expectedDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), sparkSchema)

      resultDF.schema.fields.map(_.copy(metadata = Metadata.empty)) shouldEqual
        expectedDF.schema.fields.map(_.copy(metadata = Metadata.empty))

      resultDF.collect() should contain theSameElementsAs expectedDF.collect()
    }
  }
}

object ExtendedYTsaurusStreamingTest {
  case class CustomTypesRowAsPrimitives(datetime: Long, date32: Int, datetime64: Long, timestamp64: Long,
                                        interval64: Long, uint64: Long)
}
