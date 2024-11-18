package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, LongType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.types.UInt64Long

class ExtendedYtFileFormatTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {

  it should "read primitives in any column" in {
    val data = Seq(
      Seq[Any](0L, 0.0, 0.0f, false, 0.toByte, UInt64Long(0)),
      Seq[Any](65L, 1.5, 7.2f, true, 3.toByte, UInt64Long(4)))

    val preparedData = Seq(
      Seq[Any](0L, 0.0, 0.0, false, 0L, UInt64Long(0).toLong),
      Seq[Any](65L, 1.5, 7.2, true, 3L, UInt64Long(4).toLong))

    writeTableFromURow(
      preparedData.map(x => new UnversionedRow(java.util.List.of[UnversionedValue](
        new UnversionedValue(0, ColumnValueType.INT64, false, x(0)),
        new UnversionedValue(1, ColumnValueType.DOUBLE, false, x(1)),
        new UnversionedValue(2, ColumnValueType.DOUBLE, false, x(2)),
        new UnversionedValue(3, ColumnValueType.BOOLEAN, false, x(3)),
        new UnversionedValue(4, ColumnValueType.INT64, false, x(4)),
        new UnversionedValue(5, ColumnValueType.UINT64, false, x(5)),
      ))),
      tmpPath, TableSchema.builder()
        .setUniqueKeys(false)
        .addValue("int64", ColumnValueType.ANY)
        .addValue("double", ColumnValueType.ANY)
        .addValue("float", ColumnValueType.ANY)
        .addValue("boolean", ColumnValueType.ANY)
        .addValue("int8", ColumnValueType.ANY)
        .addValue("uint64", ColumnValueType.ANY)
        .build())

    val schemaHint = StructType(Seq(
      StructField("int64", LongType), StructField("double", DoubleType), StructField("float", FloatType),
      StructField("boolean", BooleanType), StructField("int8", ByteType), StructField("uint64", UInt64Type)))

    val ans = data.map(Row.fromSeq)

    val arrowRes = spark.read.enableArrow.schemaHint(schemaHint).yt(tmpPath).collect()
    arrowRes should contain theSameElementsAs ans

    val wireRes = spark.read.disableArrow.schemaHint(schemaHint).yt(tmpPath).collect()
    wireRes should contain theSameElementsAs ans
  }

}
