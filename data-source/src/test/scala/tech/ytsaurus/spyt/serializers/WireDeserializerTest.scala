package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.typeinfo.TiType


class WireDeserializerTest extends AnyFlatSpec with Matchers {

  behavior of "WireDeserializer"

  it should "handle NULL values for columns not present in Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType),
      StructField("b", StringType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("b", TiType.string())
      .addValue("extra1", TiType.int64())
      .addValue("extra2", TiType.string())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(4)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(42L)

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.STRING)
    valueDeserializer.onBytes("hello".getBytes("UTF-8"))

    valueDeserializer.setId(2)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    valueDeserializer.setId(3)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    val row: InternalRow = deserializer.onCompleteRow()

    row.getLong(0) shouldEqual 42L
    row.getString(1) shouldEqual "hello"
  }

  it should "handle NULL values for columns present in Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType),
      StructField("b", StringType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("b", TiType.string())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(2)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.STRING)
    valueDeserializer.onBytes("test".getBytes("UTF-8"))

    val row: InternalRow = deserializer.onCompleteRow()

    row.isNullAt(0) shouldEqual true
    row.getString(1) shouldEqual "test"
  }

  it should "handle server returning columns in different order" in {
    val sparkSchema = StructType(Seq(
      StructField("b", StringType),
      StructField("a", LongType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("b", TiType.string())
      .addValue("extra", TiType.int64())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(3)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(100L)

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.STRING)
    valueDeserializer.onBytes("world".getBytes("UTF-8"))

    valueDeserializer.setId(2)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    val row: InternalRow = deserializer.onCompleteRow()

    row.getString(0) shouldEqual "world"
    row.getLong(1) shouldEqual 100L
  }

  it should "handle non-NULL values for columns not present in Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("extra", TiType.int64())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(2)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(42L)

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(999L)

    val row: InternalRow = deserializer.onCompleteRow()

    row.getLong(0) shouldEqual 42L
  }
}
