package tech.ytsaurus.spyt.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.spyt.types.{UInt64Type, YsonBinary, YsonType}
import org.apache.spark.sql.types.DataType
import tech.ytsaurus.client.rows.{UnversionedValue, WireProtocolWriteable}
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serializers.InternalRowSerializer.{writeBytes, writeHeader}
import tech.ytsaurus.spyt.serializers.{YsonRowConverter, YtLogicalType}
import tech.ytsaurus.spyt.types.YTsaurusTypes.{AddValue, BoxValue}
import tech.ytsaurus.typeinfo.{TiType, TypeName}
import tech.ytsaurus.yson.YsonConsumer
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeBuilder}

import java.io.ByteArrayOutputStream

class ExtendedYtsaurusTypes extends YTsaurusTypes {
  override def priority: Int = 1

  override def serializeValue(dataType: DataType, row: Row, i: Int, boxValue: BoxValue): UnversionedValue = {
    dataType match {
      case YsonType => boxValue(i, row.getAs[YsonBinary](i).bytes)
      case UInt64Type => boxValue(i, row.getAs[UInt64Long](i).toLong)
      case _ => throw new IllegalArgumentException(s"Data type $dataType is not supported.")
    }
  }

  override def wireDeserializeLong(dataType: DataType, value: Long, addValue: AddValue): Boolean = {
    dataType match {
      case YsonType => addValue(toYsonBytes(value))
      case UInt64Type => addValue(value)
      case _ => return false
    }
    true
  }

  override def wireDeserializeBoolean(dataType: DataType, value: Boolean, addValue: AddValue): Boolean = {
    dataType match {
      case YsonType => addValue(toYsonBytes(value))
      case _ => return false
    }
    true
  }

  override def wireDeserializeDouble(dataType: DataType, value: Double, addValue: AddValue): Boolean = {
    dataType match {
      case YsonType => addValue(toYsonBytes(value))
      case _ => false
    }
    true
  }

  override def wireDeserializeBytes(
    dataType: DataType, bytes: Array[Byte], isString: Boolean, addValue: AddValue): Boolean = {
    dataType match {
      case YsonType => if (isString) {
        addValue(toYsonBytes(bytes))
      } else {
        addValue(bytes)
      }
      case _ => return false
    }
    true
  }

  override def wireWriteRow(dataType: DataType,
                            row: InternalRow,
                            writeable: WireProtocolWriteable,
                            aggregate: Boolean,
                            idMapping: Array[Int],
                            i: Int,
                            getColumnType: Int => ColumnValueType): Boolean = {
    dataType match {
      case YsonType => writeBytes(writeable, idMapping, aggregate, i, row.getBinary(i), getColumnType)
      case UInt64Type =>
        writeHeader(writeable, idMapping, aggregate, i, 0, getColumnType)
        writeable.onInteger(row.getLong(i))
      case _ => return false
    }
    true
  }

  override def ytLogicalTypeV3(dataType: DataType): YtLogicalType = dataType match {
    case YsonType => YtLogicalType.Any
    case UInt64Type => YtLogicalType.Uint64
  }

  override def parseUInt64Value(dataType: DataType, value: Long): Any = dataType match {
    case UInt64Type => value
  }

  override def toYsonField(dataType: DataType, value: Any, consumer: YsonConsumer): Unit = dataType match {
    case UInt64Type => consumer.onUnsignedInteger(YsonRowConverter.extractValue[Long, UInt64Long](value, _.toLong))
  }

  override def sparkTypeFor(tiType: TiType): DataType = tiType.getTypeName match {
    case TypeName.Uint64 => UInt64Type
    case TypeName.Yson => YsonType
  }

  override def supportsInnerDataType(dataType: DataType): Option[Boolean] = dataType match {
    case YsonType => Some(false)
    case _ => None
  }

  override def castUInt64Value(uInt64: UInt64Long): Any = uInt64

  private def toYsonBytes(value: Any): Array[Byte] = {
    val output = new ByteArrayOutputStream(64)
    val node = new YTreeBuilder().value(value).build()
    YTreeBinarySerializer.serialize(node, output)
    output.toByteArray
  }
}
