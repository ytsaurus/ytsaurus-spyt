package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.apache.spark.sql.spyt.types.{Datetime, DatetimeType}
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter.{convertUTCtoLocal, getUtcHoursOffset}
import tech.ytsaurus.spyt.format.types.DateTimeTypesTest.structField
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TestUtils, TmpDir}

import java.sql.Timestamp
import java.time.LocalDateTime


class ExtendedDatetimeTypeTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with MockitoSugar with TableDrivenPropertyChecks with DynTableTestUtils {

  val HOURS_OFFSET: Int = getUtcHoursOffset

  val getTimestampTime: UserDefinedFunction = udf((timestamp: Timestamp) => timestamp.getTime)
  spark.udf.register("getTimestampTime", getTimestampTime)

  val datetimeArr: Seq[String] = List.apply("1970-01-01T00:00:00Z", "2019-02-09T13:41:11Z", "2038-01-19T03:14:07Z")
  val timestampArr: Seq[String] = List.apply("1970-01-01T00:00:00.000000Z", "2019-02-09T13:41:11.000000Z", "2038-01-19T03:14:07.000000Z")

  val datetimeDf: DataFrame = spark.createDataFrame(
    spark.sparkContext.parallelize(datetimeArr.map(el => Row(Datetime(LocalDateTime.parse(el.dropRight(1)))))),
    StructType(Seq(structField("datetime", new DatetimeType(), nullable = false))))

  val timestampDfAfterTSUdf: DataFrame = spark
    .createDataFrame(
      spark.sparkContext.parallelize(timestampArr.map(el => Row(convertUTCtoLocal(el, HOURS_OFFSET)))),
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

}
