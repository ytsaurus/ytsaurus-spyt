package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.spyt.types.{UInt64Type, YsonType}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.topLevelIntegers
import tech.ytsaurus.typeinfo.TiType

class ExtendedWireDeserializerTest extends AnyFlatSpec with Matchers {

  behavior of "WireDeserializer"

  private def readInteger(dataType: DataType, valueType: ColumnValueType, value: Long): Any = {
    val deserializer = new InternalRowDeserializer(StructType(Seq(StructField("v", dataType))))
    deserializer.updateSchema(TableSchema.builder().addValue("v", TiType.optional(TiType.yson())).build())
    val valueDeserializer = deserializer.onNewRow(1)
    valueDeserializer.setId(0)
    valueDeserializer.setType(valueType)
    valueDeserializer.onInteger(value)
    deserializer.onCompleteRow().get(0, dataType)
  }

  topLevelIntegers.foreach { integer =>
    it should s"read ${integer.description} of a yson column as a YSON integer of the same signedness" in {
      readInteger(new YsonType(), integer.valueType, integer.value) shouldBe integer.ysonBytes.map(_.toByte).toArray
    }
  }

  it should "read a uint64 of a uint64 column as its bits" in {
    readInteger(UInt64Type, ColumnValueType.UINT64, -1L) shouldBe -1L
  }
}
