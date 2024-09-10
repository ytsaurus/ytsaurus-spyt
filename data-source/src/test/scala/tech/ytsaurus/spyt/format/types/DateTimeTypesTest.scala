package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.spyt.types.Date32.{MAX_DATE32, MIN_DATE32}
import org.apache.spark.sql.spyt.types.Datetime64.{MAX_DATETIME64, MIN_DATETIME64}
import org.apache.spark.sql.spyt.types.Interval64.{MAX_INTERVAL64, MIN_INTERVAL64}
import org.apache.spark.sql.spyt.types.Timestamp64.{MAX_TIMESTAMP64, MIN_TIMESTAMP64}
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter._
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration, YtTableSparkSettings}
import tech.ytsaurus.spyt.format.types.DateTimeTypesTest.{rightSparkDataForWideDatetimeTypesTests, rightSparkSchemaForWideDatetimeTypesTests, rightYtDataForWideDatetimeTypesTests, rightYtSchemaForWideDatetimeTypesTests}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.{SchemaTestUtils, YtReader, YtWriter}
import tech.ytsaurus.typeinfo.TiType

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable.ListBuffer

class DateTimeTypesTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with SchemaTestUtils {
  behavior of "YtDataSource"

  val HOURS_OFFSET: Int = getUtcHoursOffset

  val ids: Seq[Long] = Seq(1L, 2L, 3L)
  val dateArr: Seq[String] = List.apply("1970-04-11", "2019-02-09", "1970-01-01")
  val datetimeArr: Seq[String] = List.apply("1970-04-11T00:00:00Z", "2019-02-09T13:41:11Z", "1970-01-01T00:00:00Z")
  val timestampArr: Seq[String] = List.apply("1970-04-11T00:00:00.000000Z", "2019-02-09T13:41:11.654321Z", "1970-01-01T00:00:00.000000Z")
  val numbers: Seq[Int] = Seq(101, 202, 303)

  it should "datetime types test: write table by yt - read by spark" in {
    val tableSchema: TableSchema = TableSchema.builder()
      .addValue("id", TiType.int64())
      .addValue("date", TiType.date())
      .addValue("datetime", TiType.datetime())
      .addValue("timestamp", TiType.timestamp())
      .addValue("number", TiType.int32())
      .build()

    val ysonData = ListBuffer[String]()
    for (i <- ids.indices) {
      ysonData +=
        s"""{id = ${ids(i)};
           |date = ${dateToLong(dateArr(i))};
           |datetime = ${datetimeToLong(datetimeArr(i))};
           |timestamp = ${zonedTimestampToLong(timestampArr(i))};
           |number = ${numbers(i)}}""".stripMargin
    }
    writeTableFromYson(ysonData, tmpPath, tableSchema)

    val df: DataFrame = spark.read.yt(tmpPath)
    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs Seq(
      StructField("id", LongType),
      StructField("date", DateType),
      StructField("datetime", new DatetimeType()),
      StructField("timestamp", TimestampType),
      StructField("number", IntegerType)
    )

    val expectedData = Seq(
      Row(
        1L,
        Date.valueOf("1970-04-11"),
        Datetime(LocalDateTime.parse("1970-04-11T00:00:00")),
        convertUTCtoLocal("1970-04-11T00:00:00.000000Z", HOURS_OFFSET),
        101
      ),
      Row(
        2L,
        Date.valueOf("2019-02-09"),
        Datetime(LocalDateTime.parse("2019-02-09T13:41:11")),
        convertUTCtoLocal("2019-02-09T13:41:11.654321Z", HOURS_OFFSET),
        202
      ),
      Row(
        3L,
        Date.valueOf("1970-01-01"),
        Datetime(LocalDateTime.parse("1970-01-01T00:00:00")),
        convertUTCtoLocal("1970-01-01T00:00:00.000000Z", HOURS_OFFSET),
        303
      )
    )

    df.collect() should contain theSameElementsAs expectedData
  }

  it should "datetime types test: write table by spark - read by yt" in {

    val schemaSpark = StructType(Seq(
      structField("id", LongType, nullable = false),
      structField("date", DateType, nullable = false),
      structField("datetime", new DatetimeType(), nullable = false),
      structField("timestamp", TimestampType, nullable = false),
      structField("number", IntegerType, nullable = false)
    ))

    val writtenBySparkData = ListBuffer[Row]()
    for (i <- ids.indices) {
      writtenBySparkData += Row(
        ids(i),
        Date.valueOf(dateArr(i)),
        Datetime(LocalDateTime.parse(datetimeArr(i).dropRight(1))),
        convertUTCtoLocal(timestampArr(i), HOURS_OFFSET),
        numbers(i)
      )
    }

    val df = spark.createDataFrame(spark.sparkContext.parallelize(writtenBySparkData), schemaSpark)
    df.write.yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
    schema.getColumnNames should contain theSameElementsAs Seq("id", "date", "datetime", "timestamp", "number")
    schema.getColumnType(1) shouldEqual ColumnValueType.UINT64
    schema.getColumnType(2) shouldEqual ColumnValueType.UINT64
    schema.getColumnType(3) shouldEqual ColumnValueType.UINT64
    schema.getColumnType(4) shouldEqual ColumnValueType.INT64

    val expectedSchema = TableSchema.builder().setUniqueKeys(false)
      .addValue("id", TiType.optional(TiType.int64()))
      .addValue("date", TiType.optional(TiType.date()))
      .addValue("datetime", TiType.optional(TiType.datetime()))
      .addValue("timestamp", TiType.optional(TiType.timestamp()))
      .addValue("number", TiType.optional(TiType.int32()))
      .build()

    schema shouldEqual expectedSchema

    val data = readTableAsYson(tmpPath).map { yson =>
      val map = yson.asMap()
      (
        map.get("id").longValue(),
        daysToDate(map.get("date").longValue()),
        longToDatetime(map.get("datetime").longValue()),
        longToZonedTimestamp(map.get("timestamp").longValue()),
        map.get("number").intValue()
      )
    }

    val expectedData = ListBuffer[(Long, java.sql.Date, String, String, Int)](
      (1L, Date.valueOf("1970-04-11"), "1970-04-11T00:00:00Z", "1970-04-11T00:00:00.000000Z", 101),
      (2L, Date.valueOf("2019-02-09"), "2019-02-09T13:41:11Z", "2019-02-09T13:41:11.654321Z", 202),
      (3L, Date.valueOf("1970-01-01"), "1970-01-01T00:00:00Z", "1970-01-01T00:00:00.000000Z", 303)
    )

    data should contain theSameElementsAs expectedData
  }

  it should "wide datetime types: write by spark, read by yt" in {
    withConf(SparkYtConfiguration.Schema.ForcingNullableIfNoMetadata, false) {
      val df_0 = spark.createDataFrame(
        spark.sparkContext.parallelize(rightSparkDataForWideDatetimeTypesTests),
        rightSparkSchemaForWideDatetimeTypesTests)

      df_0.write
        .option(YtTableSparkSettings.WriteTypeV3.name, value = true)
        .option(YtTableSparkSettings.NullTypeAllowed.name, value = false)
        .option(YtUtils.Options.PARSING_TYPE_V3, value = true)
        .yt(tmpPath)

      val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))

      schema shouldEqual rightYtSchemaForWideDatetimeTypesTests

      val data = readTableAsYson(tmpPath).map { yson =>
        val map = yson.asMap()
        (
          Date32(map.get("date32").intValue()),
          Datetime64(map.get("datetime64").longValue()),
          Timestamp64(map.get("timestamp64").longValue()),
          Interval64(map.get("interval64").longValue())
        )
      }

      data should contain theSameElementsAs rightYtDataForWideDatetimeTypesTests
    }
  }

  it should "wide datetime types: write by yt, read by spark" in {
    withConf(SparkYtConfiguration.Schema.ForcingNullableIfNoMetadata, false) {
      val ysonData = ListBuffer[String]()
      for (i <- rightYtDataForWideDatetimeTypesTests.indices){
        ysonData +=
          s"""{date32 = ${rightYtDataForWideDatetimeTypesTests(i)._1.toInt};
             |datetime64 = ${rightYtDataForWideDatetimeTypesTests(i)._2.toLong};
             |timestamp64 = ${rightYtDataForWideDatetimeTypesTests(i)._3.toLong};
             |interval64 = ${rightYtDataForWideDatetimeTypesTests(i)._4.toLong};}""".stripMargin
      }
      writeTableFromYson(ysonData, tmpPath, rightYtSchemaForWideDatetimeTypesTests)

      val df_1 = spark.read
        .option(YtTableSparkSettings.NullTypeAllowed.name, value = false)
        .option(YtUtils.Options.PARSING_TYPE_V3, value = true)
        .yt(tmpPath)

      df_1.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs
        rightSparkSchemaForWideDatetimeTypesTests
      df_1.collect() should contain theSameElementsAs rightSparkDataForWideDatetimeTypesTests
    }
  }
}


object DateTimeTypesTest extends SchemaTestUtils {
  private val rightSparkSchemaForWideDatetimeTypesTests: StructType = {
    StructType(Seq(
      StructField("date32", new Date32Type(), nullable = false),
      StructField("datetime64", new Datetime64Type(), nullable = false),
      StructField("timestamp64", new Timestamp64Type(), nullable = false),
      StructField("interval64", new Interval64Type(), nullable = false),
    ))
  }

  private val rightYtSchemaForWideDatetimeTypesTests: TableSchema = TableSchema.builder().setUniqueKeys(false)
    .addValue("date32", TiType.date32())
    .addValue("datetime64", TiType.datetime64())
    .addValue("timestamp64", TiType.timestamp64())
    .addValue("interval64", TiType.interval64())
    .build()

  val date32Arr: Seq[Int] = List.apply(MIN_DATE32, 0, MAX_DATE32)
  val datetime64Arr: Seq[Long] = List.apply(MIN_DATETIME64, 0L, MAX_DATETIME64)
  val timestamp64Arr: Seq[Long] = List.apply(MIN_TIMESTAMP64, 0L, MAX_TIMESTAMP64)
  val interval64Arr: Seq[Long] = List.apply(MIN_INTERVAL64, 0L, MAX_INTERVAL64)

  val date32Now: LocalDate = LocalDate.now()
  val datetime64Now: LocalDateTime = LocalDateTime.now()
  val timestamp64Now: Timestamp = Timestamp.valueOf("2024-08-29 11:30:45.987654")

  private val rightSparkDataForWideDatetimeTypesTests: Seq[Row] = Seq(
    Row(Date32(date32Arr.head), Datetime64(datetime64Arr.head), Timestamp64(timestamp64Arr.head),
      Interval64(interval64Arr.head)),
    Row(Date32(date32Arr(1)), Datetime64(datetime64Arr(1)), Timestamp64(timestamp64Arr(1)),
      Interval64(interval64Arr(1))),
    Row(Date32(date32Arr(2)), Datetime64(datetime64Arr(2)), Timestamp64(timestamp64Arr(2)),
      Interval64(interval64Arr(2))),
    Row(Date32(date32Now), Datetime64(datetime64Now), Timestamp64(timestamp64Now), Interval64(1234567890L))
  )

  private val rightYtDataForWideDatetimeTypesTests: List[(Date32, Datetime64, Timestamp64, Interval64)] = List.apply(
    (Date32(date32Arr.head), Datetime64(datetime64Arr.head), Timestamp64(timestamp64Arr.head),
      Interval64(interval64Arr.head)),
    (Date32(date32Arr(1)), Datetime64(datetime64Arr(1)), Timestamp64(timestamp64Arr(1)), Interval64(interval64Arr(1))),
    (Date32(date32Arr(2)), Datetime64(datetime64Arr(2)), Timestamp64(timestamp64Arr(2)), Interval64(interval64Arr(2))),
    (Date32(date32Now), Datetime64(datetime64Now), Timestamp64(timestamp64Now), Interval64(1234567890L))
  )
}
