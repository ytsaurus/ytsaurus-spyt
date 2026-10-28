package tech.ytsaurus.spyt

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.v2.{YtReaderOptions, YtScan}
import org.scalatest.Assertions
import org.scalatest.flatspec.AnyFlatSpec

import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.YsonAsVariant
import tech.ytsaurus.spyt.serializers.SchemaConverterConfig.VARIANT_SPARK_VERSION
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.spyt.test.LocalSpark
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeTextSerializer}

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._

trait YsonAsVariantTestUtils extends AnyFlatSpec with LocalSpark {

  val anyColumnSchema: TableSchema = TableSchema.builder().setUniqueKeys(false)
    .addValue("id", ColumnValueType.INT64)
    .addValue("value", ColumnValueType.ANY)
    .build()

  def usualYsonType: DataType = YtLogicalType.Any.sparkType.topLevel

  def ysonColumnType(ysonAsVariant: Boolean): DataType = {
    if (ysonAsVariant) SparkAdapter.instance.ysonVariantConverter.variantType else usualYsonType
  }

  def testWithYsonAsVariant(testName: String)(testBody: Boolean => Unit): Unit = {
    it should s"$testName: ysonAsVariant = false" in {
      testBody(false)
    }

    it should s"$testName: ysonAsVariant = true" in {
      YsonAsVariantTestUtils.whenVariantSupported {
        withConf(YsonAsVariant, true) {
          testBody(true)
        }
      }
    }
  }

  def variantJson(df: DataFrame, column: String): Seq[String] = {
    df.selectExpr(s"to_json(`$column`)").collect().map(_.getString(0)).toSeq
  }

  def ysonText(bytes: Array[Byte]): String = {
    YTreeTextSerializer.stableSerialize(YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytes)))
  }

  def canonicalYson(text: String): String = {
    YTreeTextSerializer.stableSerialize(YTreeTextSerializer.deserialize(text))
  }

  def ysonBytes(text: String): Array[Byte] = YTreeTextSerializer.deserialize(text).toBinary

  def unversionedRow(values: (ColumnValueType, Any)*): UnversionedRow = {
    new UnversionedRow(values.zipWithIndex.map { case ((valueType, value), id) =>
      new UnversionedValue(id, valueType, false, value)
    }.asJava)
  }

  def anyColumnRows(cells: Seq[(ColumnValueType, Any)]): Seq[UnversionedRow] = {
    cells.zipWithIndex.map { case (cell, index) => unversionedRow(ColumnValueType.INT64 -> (index + 1L), cell) }
  }

  def byId[T](values: Seq[T]): Map[Long, T] = {
    values.zipWithIndex.map { case (value, index) => (index + 1L) -> value }.toMap
  }

  def columnTexts(df: DataFrame, key: String, column: String, ysonAsVariant: Boolean): Seq[(Long, String)] = {
    if (ysonAsVariant) {
      df.selectExpr(key, s"to_json(`$column`)").collect().map(row => row.getLong(0) -> row.getString(1)).toSeq
    } else {
      df.selectExpr(key, s"cast(`$column` AS binary)").collect().map { row =>
        row.getLong(0) -> Option(row.getAs[Array[Byte]](1)).map(ysonText).orNull
      }.toSeq
    }
  }

  def textsById(df: DataFrame, ysonAsVariant: Boolean): Map[Long, String] = {
    columnTexts(df, "id", "value", ysonAsVariant).toMap
  }

  def executedPlanNodes[T](df: DataFrame)(node: PartialFunction[SparkPlan, T]): Seq[T] = {
    YsonAsVariantTestUtils.AdaptivePlans.collect(df.queryExecution.executedPlan)(node)
  }

  def readsArrowBatches(df: DataFrame): Boolean = {
    val scan = executedPlanNodes(df) { case batchScan: BatchScanExec => batchScan.scan }
      .collectFirst { case ytScan: YtScan => ytScan }.get
    YtReaderOptions.canReadBatch(scan.readDataSchema, scan.options.asScala.toMap, spark.sessionState.conf)
  }
}

object YsonAsVariantTestUtils {

  case class TopLevelInteger(description: String, valueType: ColumnValueType, value: Long, ysonBytes: Seq[Int])

  // Binary YSON of an integer: 0x06 for uint64 or 0x02 for int64, then the value as a varint, zigzag-encoded for int64
  val topLevelIntegers: Seq[TopLevelInteger] = Seq(
    TopLevelInteger("a small uint64", ColumnValueType.UINT64, 5L, Seq(0x06, 0x05)),
    TopLevelInteger("the largest uint64", ColumnValueType.UINT64, -1L, 0x06 +: Seq.fill(9)(0xff) :+ 0x01),
    TopLevelInteger("an int64", ColumnValueType.INT64, -1L, Seq(0x02, 0x01)))

  def whenVariantSupported(testBody: => Unit): Unit = {
    if (SparkVersionUtils.greaterThanOrEqual(VARIANT_SPARK_VERSION)) {
      testBody
    } else {
      Assertions.cancel(s"Spark ${SparkVersionUtils.currentVersion} has no Variant type")
    }
  }

  private object AdaptivePlans extends AdaptiveSparkPlanHelper
}
