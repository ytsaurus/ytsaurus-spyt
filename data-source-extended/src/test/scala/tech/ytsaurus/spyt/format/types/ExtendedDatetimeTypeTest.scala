package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.apache.spark.sql.spyt.types.{Datetime, DatetimeType}
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.{convertUTCtoLocal, datetimeToLong}
import tech.ytsaurus.spyt.test.DynTableTestUtils
import tech.ytsaurus.typeinfo.TiType

import java.sql.Timestamp
import java.time.LocalDateTime


class ExtendedDatetimeTypeTest extends DateTimeTypesTestBase with MockitoSugar with TableDrivenPropertyChecks
  with DynTableTestUtils {

  val getTimestampTime: UserDefinedFunction = udf((timestamp: Timestamp) => timestamp.getTime)
  spark.udf.register("getTimestampTime", getTimestampTime)

  val datetimeDf: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(datetimeArr.map(el => Row(Datetime(LocalDateTime.parse(el.dropRight(1)))))),
    StructType(Seq(structField("datetime", new DatetimeType(), nullable = false))))

  private val truncatedTimestampArr = timestampArr.map { el =>
    val ts = convertUTCtoLocal(el, HOURS_OFFSET)
    ts.setNanos(0)
    ts.setTime(ts.getTime / 1000 * 1000)
    Row(ts)
  }

  val timestampDfAfterTSUdf: DataFrame = spark
    .createDataFrame(
      spark.sparkContext.parallelize(truncatedTimestampArr),
      StructType(Seq(structField("timestamp", TimestampType, nullable = false))))
    .withColumn("millis", getTimestampTime(col("timestamp")))
    .select("millis")

  private def autoCastDfColumnTemplate(): Unit = {
    val datetimeDfAfterTSUdf = datetimeDf.withColumn("millis", getTimestampTime(col("datetime"))).select("millis")
    datetimeDfAfterTSUdf.collect() should contain theSameElementsAs timestampDfAfterTSUdf.collect()
    datetimeDfAfterTSUdf.schema === timestampDfAfterTSUdf.schema
  }

  it should "auto cast DatetimeType to TimestampType by spark df" in {
    autoCastDfColumnTemplate()
  }

  it should "auto cast DatetimeType to TimestampType by spark df with disabled codegen" in {
    spark.conf.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    spark.conf.set(CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.NO_CODEGEN.toString)
    sys.props += "spark.testing" -> "true"

    try {
      autoCastDfColumnTemplate()
    } finally {
      spark.conf.set(WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      spark.conf.set(CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.FALLBACK.toString)
      sys.props -= "spark.testing"
    }
  }

  it should "auto cast DatetimeType to TimestampType by spark sql" in {
    datetimeDf.createOrReplaceTempView("dt_to_ts_view")
    val datetimeDfAfterTSUdf = spark.sql("SELECT getTimestampTime(datetime) as year FROM dt_to_ts_view")
    datetimeDfAfterTSUdf.collect() should contain theSameElementsAs timestampDfAfterTSUdf.collect()
    datetimeDfAfterTSUdf.schema === timestampDfAfterTSUdf.schema
  }

  it should "work correctly with caching dataframes containing datetime types columns" in {
    readTestTemplate(_.cache())
  }

  private val storageLevels = List("NONE", "DISK_ONLY", "DISK_ONLY_2", "DISK_ONLY_3", "MEMORY_ONLY", "MEMORY_ONLY_2",
    "MEMORY_ONLY_SER", "MEMORY_ONLY_SER_2", "MEMORY_AND_DISK", "MEMORY_AND_DISK_2", "MEMORY_AND_DISK_SER",
    "MEMORY_AND_DISK_SER_2", "OFF_HEAP")

  storageLevels.foreach { storageLevel =>
    it should s"work correctly with persisting dataframes containing datetime types columns using $storageLevel " +
      s"StorageLevel" in {
      readTestTemplate(_.persist(StorageLevel.fromString(storageLevel)))
    }
  }

  it should s"execute SortAggregate on dataframe with datetime and string columns" in {
    val schema: TableSchema = TableSchema.builder()
      .addValue("id", TiType.int64())
      .addValue("value", TiType.string())
      .addValue("datetime", TiType.datetime())
      .build()
    val data = datetimeArr.zipWithIndex.map { case (datetime, index) =>
      s"""{id=1;value="value_$index";datetime=${datetimeToLong(datetime)}}"""
    }
    writeTableFromYson(data, tmpPath, schema)

    val res = withConf(org.apache.spark.sql.internal.SQLConf.USE_OBJECT_HASH_AGG, "false") {
      spark.read.yt(tmpPath).dropDuplicates("id").collect()
    }

    res.length shouldEqual 1
    res should contain oneElementOf Seq(
      Row(1, "value_0", Datetime(LocalDateTime.parse("1970-04-11T00:00:00"))),
      Row(1, "value_1", Datetime(LocalDateTime.parse("2019-02-09T13:41:11"))),
      Row(1, "value_2", Datetime(LocalDateTime.parse("1970-01-01T00:00:00")))
    )
  }
}
