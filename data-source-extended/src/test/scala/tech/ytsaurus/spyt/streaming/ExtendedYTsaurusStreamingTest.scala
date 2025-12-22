package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils.Options.PARSING_TYPE_V3
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.request.ModifyRowsRequest
import tech.ytsaurus.core.tables._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.WriteTypeV3
import tech.ytsaurus.spyt.streaming.ExtendedYTsaurusStreamingTest.normalizeRow
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.types.UInt64Long
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTree

import java.util.UUID
import scala.collection.JavaConverters._
import scala.language.postfixOps


class ExtendedYTsaurusStreamingTest extends AnyFlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils
  with TmpDir with DynTableTestUtils with QueueTestUtils {

  import tech.ytsaurus.spyt._

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
        .addValue("yson", TiType.optional(TiType.yson()))
        .addValue("list", TiType.list(TiType.int64()))
        .addValue("struct", TiType.struct(
          new Member("a", TiType.int64()),
          new Member("b", TiType.utf8())
        ))
        .build().toWrite

      YtWrapper.createDir(tmpPath)
      prepareOrderedTestTable(queuePath, ytSchema, enableDynamicStoreRead = true)
      prepareConsumer(consumerPath, queuePath)
      waitQueueRegistration(queuePath)
      prepareOrderedTestTable(resultPath, ytSchema, enableDynamicStoreRead = true)

      (0 until recordCountLimit.toInt).foreach { i =>
        val id = i.toLong

        val listNode = YTree.listBuilder.value(i).value(i + 1).buildList

        val structNode = YTree.listBuilder
          .value(id)
          .value(s"str_$i")
          .buildList

        val ysonVal = Array[Byte](1, 2, 3)

        val req = ModifyRowsRequest.builder.setPath(queuePath).setSchema(ytSchema).addInsert(
          java.util.List.of(
            id: java.lang.Long,
            id: java.lang.Long,
            id: java.lang.Long,
            id: java.lang.Long,
            id: java.lang.Long,
            id: java.lang.Long,
            ysonVal,
            listNode,
            structNode)
        ).build
        YtWrapper.insertRows(req, None)
      }


      val df: DataFrame = spark
        .readStream
        .format("yt")
        .option(PARSING_TYPE_V3, value = true)
        .option("consumer_path", consumerPath)
        .load(queuePath)
      val query = df
        .writeStream
        .option(WriteTypeV3.name, value = true)
        .option("checkpointLocation", f"yt:/$tmpPath/checkpoints_${UUID.randomUUID()}")
        .trigger(ProcessingTime(2000))
        .format("yt")
        .option("path", resultPath)
        .start()

      var currentCount = 0L
      val startTime = System.currentTimeMillis()
      while (currentCount < recordCountLimit && (System.currentTimeMillis() - startTime) < 120 * 1000) {
        Thread.sleep(2000)
        currentCount = spark.read.option(PARSING_TYPE_V3, value = true).option("enable_inconsistent_read", "true").yt(resultPath).count()
      }

      query.stop()

      val resultDF = spark.read
        .option(PARSING_TYPE_V3, value = true)
        .option("enable_inconsistent_read", "true")
        .yt(resultPath)
        .orderBy("uint64")

      val expectedRows = (0 until recordCountLimit.toInt).map { i =>
        val id = i.toLong
        Row(
          Datetime(id),
          new Date32(i),
          new Datetime64(id),
          new Timestamp64(id),
          new Interval64(id),
          new UInt64Long(id),
          Array[Byte](1, 2, 3),
          Seq(id, id + 1),
          Row(id, s"str_$i")
        )
      }

      val intermediateSchema = StructType(Seq(
        StructField("datetime", new DatetimeType(), nullable = false),
        StructField("date32", new Date32Type(), nullable = false),
        StructField("datetime64", new Datetime64Type(), nullable = false),
        StructField("timestamp64", new Timestamp64Type(), nullable = false),
        StructField("interval64", new Interval64Type(), nullable = false),
        StructField("uint64", UInt64Type, nullable = false),
        StructField("yson", BinaryType),
        StructField("list", ArrayType(LongType, containsNull = false), nullable = false),
        StructField("struct", StructType(Seq(
          StructField("a", LongType, nullable = false),
          StructField("b", StringType, nullable = false)
        )), nullable = false)
      ))

      val tempDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedRows),
        intermediateSchema
      )

      val expectedDF = tempDF.withColumn("yson", col("yson").cast(YsonType))

      resultDF.schema.fields.map(_.copy(metadata = Metadata.empty)) shouldEqual
        expectedDF.schema.fields.map(_.copy(metadata = Metadata.empty))

      val resultData = resultDF.collect().map(normalizeRow)
      val expectedData = expectedDF.collect().map(normalizeRow)

      resultData should contain theSameElementsAs expectedData
    }
  }
}

object ExtendedYTsaurusStreamingTest {

  def normalizeRow(value: Any): Any = value match {
    case row: Row => row.toSeq.map(normalizeRow)
    case bytes: Array[Byte] => bytes.toList
    case yson: YsonBinary => yson.bytes.toList
    case seq: Iterable[_] => seq.map(normalizeRow).toList
    case javaList: java.util.List[_] => javaList.asScala.map(normalizeRow).toList
    case javaMap: java.util.Map[_, _] => javaMap.asScala.mapValues(normalizeRow).toMap
    case other => other
  }
}
