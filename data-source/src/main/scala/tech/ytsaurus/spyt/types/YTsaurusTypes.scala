package tech.ytsaurus.spyt.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, DataType, Decimal, DecimalType}
import tech.ytsaurus.client.rows.{UnversionedValue, WireProtocolWriteable}
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.spyt.types.YTsaurusTypes.{AddValue, BoxValue}
import tech.ytsaurus.typeinfo.{TiType, TypeName}
import tech.ytsaurus.yson.YsonConsumer

import java.util.ServiceLoader
import scala.collection.JavaConverters._

trait YTsaurusTypes {
  /**
   * Implementation with higher priority will take the precedence.
   *
   * @return
   */
  def priority: Int

  def serializeValue(dataType: DataType, row: Row, i: Int, boxValue: BoxValue): UnversionedValue

  def wireDeserializeLong(dataType: DataType, value: Long, addValue: AddValue): Boolean
  def wireDeserializeBoolean(dataType: DataType, value: Boolean, addValue: AddValue): Boolean
  def wireDeserializeDouble(dataType: DataType, value: Double, addValue: AddValue): Boolean
  def wireDeserializeBytes(dataType: DataType, bytes: Array[Byte], isString: Boolean, addValue: AddValue): Boolean
  def wireWriteRow(dataType: DataType,
                   row: InternalRow,
                   writeable: WireProtocolWriteable,
                   aggregate: Boolean,
                   idMapping: Array[Int],
                   i: Int,
                   getColumnType: Int => ColumnValueType): Boolean

  def ytLogicalTypeV3(dataType: DataType): YtLogicalType

  def toYsonField(dataType: DataType, value: Any, consumer: YsonConsumer): Unit

  def parseUInt64Value(dataType: DataType, value: Long): Any

  def sparkTypeFor(tiType: TiType): DataType

  def supportsInnerDataType(dataType: DataType): Option[Boolean]
}

object YTsaurusTypes {
  lazy val instance: YTsaurusTypes = ServiceLoader.load(classOf[YTsaurusTypes]).asScala.reduce { (impl1, impl2) =>
    if (impl1.priority > impl2.priority) impl1 else impl2
  }

  type BoxValue = (Int, Any) => UnversionedValue
  type AddValue = Any => Unit

  val DECIMAL_2_POW_64: Decimal = Decimal("18446744073709551616")

  def longToUnsignedDecimal(longValue: Long): Decimal = {
    val decValue = Decimal(longValue, 20, 0)
    if (longValue >= 0) decValue else YTsaurusTypes.DECIMAL_2_POW_64 + decValue
  }

  val UINT64_DEC_TYPE: DataType = DecimalType(20, 0)
}

class BaseYTsaurusTypes extends YTsaurusTypes {
  override def priority: Int = 0

  override def serializeValue(dataType: DataType,
                              row: Row,
                              i: Int,
                              boxValue: (Int, Any) => UnversionedValue): UnversionedValue = {
    typeNotSupported(dataType)
  }

  override def wireDeserializeLong(dataType: DataType, value: Long, addValue: AddValue): Boolean = false

  override def wireDeserializeBoolean(dataType: DataType, value: Boolean, addValue: AddValue): Boolean = false

  override def wireDeserializeDouble(dataType: DataType, value: Double, addValue: AddValue): Boolean = false

  override def wireDeserializeBytes(
      dataType: DataType, bytes: Array[Byte], isString: Boolean, addValue: AddValue): Boolean = false

  override def wireWriteRow(dataType: DataType, row: InternalRow, writeable: WireProtocolWriteable,
                            aggregate: Boolean, idMapping: Array[Int], i: Int,
                            getColumnType: Int => ColumnValueType): Boolean = false

  override def ytLogicalTypeV3(dataType: DataType): YtLogicalType = typeNotSupported(dataType)

  override def toYsonField(dataType: DataType, value: Any, consumer: YsonConsumer): Unit = typeNotSupported(dataType)

  override def parseUInt64Value(dataType: DataType, value: Long): Decimal = typeNotSupported(dataType)

  override def sparkTypeFor(tiType: TiType): DataType = tiType.getTypeName match {
    case TypeName.Uint64 => YTsaurusTypes.UINT64_DEC_TYPE
    case TypeName.Yson => BinaryType
  }

  override def supportsInnerDataType(dataType: DataType): Option[Boolean] = None

  private def typeNotSupported(dataType: DataType): Nothing = {
    throw new IllegalArgumentException(s"Data type $dataType is not supported. Maybe you should put " +
      s"data-source-extended module on classpath")
  }
}
