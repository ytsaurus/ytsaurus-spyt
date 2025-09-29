package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import tech.ytsaurus.client.rows.{WireRowDeserializer, WireValueDeserializer}
import tech.ytsaurus.core.common.Decimal.binaryToText
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.common.utils.UuidUtils
import tech.ytsaurus.spyt.serialization.YsonDecoder
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.types.YTsaurusTypes

import java.nio.charset.StandardCharsets


abstract class WireDeserializer[T](schema: StructType) extends WireRowDeserializer[T] with WireValueDeserializer[Any] {
  protected var _values: Array[Any] = _
  private val indexedSchema = schema.fields.map(_.dataType).toIndexedSeq
  private val indexedDataTypes = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))
  private var serverIdToSparkIndex: Array[Int] = _

  private var _currentType: ColumnValueType = ColumnValueType.THE_BOTTOM
  private var _index = 0

  override def updateSchema(ytSchema: TableSchema): Unit = {
    val sparkFieldIndex: Map[String, Int] = this.schema.fields.map(_.name).zipWithIndex.toMap
    serverIdToSparkIndex = new Array[Int](ytSchema.getColumns.size())
    for (i <- 0 until ytSchema.getColumns.size()) {
      val name = ytSchema.getColumns.get(i).getName
      serverIdToSparkIndex(i) = sparkFieldIndex.getOrElse(name, -1)
    }
  }

  override def onNewRow(columnCount: Int): WireValueDeserializer[_] = {
    _values = new Array[Any](schema.length)
    _index = 0
    _currentType = ColumnValueType.THE_BOTTOM
    this
  }

  override def onNullRow(): T = throw new IllegalArgumentException("Null rows are not supported")

  override def setId(id: Int): Unit = {
    if (id >= 0 && id < serverIdToSparkIndex.length) {
      _index = serverIdToSparkIndex(id)
    } else {
      _index = -1
    }
  }

  override def setType(`type`: ColumnValueType): Unit = {
    _currentType = `type`
  }

  override def setAggregate(aggregate: Boolean): Unit = {}

  override def setTimestamp(timestamp: Long): Unit = {}

  override def build(): Any = null

  private def addValue(value: Any): Unit = {
    _values(_index) = value
  }

  override def onEntity(): Unit = addValue(null)

  override def onInteger(value: Long): Unit = {
    if (isIndexValid) {
      _currentType match {
        case ColumnValueType.INT64 | ColumnValueType.UINT64 =>
          indexedSchema(_index) match {
            case LongType => addValue(value)
            case IntegerType => addValue(value.toInt)
            case ShortType => addValue(value.toShort)
            case ByteType => addValue(value.toByte)
            case DateType => addValue(value.toInt)
            case _: DatetimeType => addValue(value)
            case TimestampType => addValue(value)
            case _: Date32Type => addValue(value.toInt)
            case _: Datetime64Type => addValue(value)
            case _: Timestamp64Type => addValue(value)
            case _: Interval64Type => addValue(value)
            case _: DecimalType => (_currentType: @unchecked) match {
              case ColumnValueType.INT64 => addValue(Decimal(value))
              case ColumnValueType.UINT64 => addValue(YTsaurusTypes.longToUnsignedDecimal(value))
            }
            case FloatType => addValue(value.toFloat)
            case DoubleType => addValue(value.toDouble)
            case BooleanType => addValue(value != 0L)
            case StringType => addValue(UTF8String.fromString(value.toString))
            case otherType => if (!YTsaurusTypes.instance.wireDeserializeLong(otherType, value, addValue)) {
              throwSchemaViolation()
            }
          }
        case _ => throwValueTypeViolation("integer")
      }
    }
  }

  override def onBoolean(value: Boolean): Unit = {
    if (isIndexValid) {
      _currentType match {
        case ColumnValueType.BOOLEAN =>
          indexedSchema(_index) match {
            case LongType => addValue(if (value) 1L else 0L)
            case IntegerType => addValue(if (value) 1 else 0)
            case BooleanType => addValue(value)
            case StringType => addValue(UTF8String.fromString(value.toString))
            case otherType => if (!YTsaurusTypes.instance.wireDeserializeBoolean(otherType, value, addValue)) {
              throwSchemaViolation()
            }
          }
        case _ => throwValueTypeViolation("boolean")
      }
    }
  }

  override def onDouble(value: Double): Unit = {
    if (isIndexValid) {
      _currentType match {
        case ColumnValueType.DOUBLE =>
          indexedSchema(_index) match {
            case LongType => addValue(value.toLong)
            case IntegerType => addValue(value.toInt)
            case FloatType => addValue(value.toFloat)
            case DoubleType => addValue(value)
            case StringType => addValue(UTF8String.fromString(value.toString))
            case otherType => if (!YTsaurusTypes.instance.wireDeserializeDouble(otherType, value, addValue)) {
              throwSchemaViolation()
            }
          }
        case _ => throwValueTypeViolation("double")
      }
    }
  }

  private def getString(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)

  override def onBytes(bytes: Array[Byte]): Unit = {
    if (isIndexValid) {
      _currentType match {
        case ColumnValueType.STRING =>
          indexedSchema(_index) match {
            case LongType => addValue(getString(bytes).toLong)
            case IntegerType => addValue(getString(bytes).toInt)
            case FloatType => addValue(getString(bytes).toFloat)
            case DoubleType => addValue(getString(bytes).toDouble)
            case BooleanType => addValue(getString(bytes).toBoolean)
            case BinaryType => addValue(bytes)
            case StringType => addValue(getBytesFromStringType(bytes))
            case d: DecimalType =>
              addValue(Decimal(BigDecimal(binaryToText(bytes, d.precision, d.scale)), d.precision, d.scale))
            case otherType => if (!YTsaurusTypes.instance.wireDeserializeBytes(otherType, bytes, isString = true, addValue)) {
              throwSchemaViolation()
            }
          }
        case ColumnValueType.ANY | ColumnValueType.COMPOSITE =>
          indexedSchema(_index) match {
            case _@(ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
              addValue(YsonDecoder.decode(bytes, indexedDataTypes(_index)))
            case BinaryType => addValue(bytes)
            case otherType => if (!YTsaurusTypes.instance.wireDeserializeBytes(otherType, bytes, isString = false, addValue)) {
              throwSchemaViolation()
            }
          }
        case _ => throwValueTypeViolation("string")
      }
    }
  }

  private def getBytesFromStringType(bytes: Array[Byte]): UTF8String = {
    val metadata = schema.fields(_index).metadata
    val ytType = MetadataFields.YT_LOGICAL_TYPE
    if (metadata.contains(ytType) && metadata.getString(ytType) == YtLogicalType.Uuid.name) {
      UuidUtils.bytesToUTF8Uuid(bytes)
    } else {
      UTF8String.fromBytes(bytes)
    }
  }

  private def throwSchemaViolation(): Unit = {
    throw new IllegalArgumentException(s"Value type ${_currentType} does not match schema data type ${indexedSchema(_index)}")
  }

  private def throwValueTypeViolation(ysonType: String): Unit = {
    throw new IllegalArgumentException(s"Value of YSON type $ysonType does not match value type ${_currentType}")
  }

  private def isIndexValid: Boolean = {
    _index >= 0 && _index < _values.length
  }
}
