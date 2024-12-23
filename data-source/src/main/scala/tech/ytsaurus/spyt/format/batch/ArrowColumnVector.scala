package tech.ytsaurus.spyt.format.batch

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{BaseRepeatedValueVector, ListVector, StructVector}
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String
import tech.ytsaurus.core.common.Decimal.binaryToText
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serialization.IndexedDataType.{ArrayType => IArrayType, AtomicType => IAtomicType}
import tech.ytsaurus.spyt.serialization.{IndexedDataType, YsonDecoder}
import tech.ytsaurus.spyt.types.YTsaurusTypes
import tech.ytsaurus.yson.YsonTags

import java.nio.charset.StandardCharsets

class ArrowColumnVector(dataType: IndexedDataType,
                        vector: ValueVector,
                        dictionary: Option[Dictionary],
                        isNullVector: Boolean,
                        columnType: ColumnValueType) extends ColumnVector(dataType.sparkDataType) {
  private val accessor: ArrowVectorAccessor = {
    if (isNullVector) {
      NullAccessor
    } else {
      val keys = dictionary.map { _ =>
        vector match {
          case v: BaseIntVector => v
          case e => throw new UnsupportedOperationException(
            f"Unexpected vector for column `${e.getName}`: ${e.getClass}"
          )
        }
      }

      val values = dictionary.map(_.getVector).getOrElse(vector)

      values match {
        case v: BitVector => BooleanAccessor(keys, v)
        case v: TinyIntVector => ByteAccessor(keys, v)
        case v: SmallIntVector => ShortAccessor(keys, v)
        case v: IntVector => IntAccessor(keys, v)
        case v: UInt1Vector => UInt1Accessor(keys, v)
        case v: UInt2Vector => UInt2Accessor(keys, v)
        case v: UInt4Vector => UInt4Accessor(keys, v)
        case v: UInt8Vector => UInt8Accessor(keys, v)
        case v: BigIntVector => LongAccessor(keys, v)
        case v: Float4Vector => FloatAccessor(keys, v)
        case v: Float8Vector => DoubleAccessor(keys, v)
        case v: DecimalVector => DecimalAccessor(keys, v)
        case v: VarCharVector => StringAccessor(keys, v)
        case v: VarBinaryVector
          if columnType == ColumnValueType.STRING && !dataType.sparkDataType.isInstanceOf[DecimalType] =>
          BinaryAccessor(keys, v)
        case v: VarBinaryVector => YsonAccessor(keys, v)
        case v: DateDayVector => DateAccessor(keys, v)
        case v: TimeStampMicroTZVector => TimestampAccessor(keys, v)
        case v: ListVector => ArrayAccessor(keys, v)
        case v: StructVector => StructAccessor(keys, v)
        case _ => throw new UnsupportedOperationException
      }
    }
  }

  private var childColumns: Array[ColumnVector] = _

  override def hasNull: Boolean = accessor.getNullCount > 0

  override def numNulls: Int = accessor.getNullCount

  override def close(): Unit = {
    if (childColumns != null) {
      childColumns.indices.foreach { i =>
        childColumns(i).close()
        childColumns(i) = null
      }
      childColumns = null
    }
    accessor.close()
  }

  override def isNullAt(rowId: Int): Boolean = accessor.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = accessor.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = accessor.getByte(rowId)

  override def getShort(rowId: Int): Short = accessor.getShort(rowId)

  override def getInt(rowId: Int): Int = accessor.getInt(rowId)

  override def getLong(rowId: Int): Long = accessor.getLong(rowId)

  override def getFloat(rowId: Int): Float = accessor.getFloat(rowId)

  override def getDouble(rowId: Int): Double = accessor.getDouble(rowId)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    accessor.getDecimal(rowId, precision, scale)
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    accessor.getUTF8String(rowId)
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    accessor.getBinary(rowId)
  }

  override def getArray(rowId: Int): ColumnarArray = {
    if (isNullAt(rowId)) {
      null
    } else {
      accessor.getArray(rowId)
    }
  }

  override def getMap(rowId: Int): ColumnarMap = throw new UnsupportedOperationException

  override def getChild(ordinal: Int): ColumnVector = childColumns(ordinal)

  abstract private class ArrowVectorAccessor {
    def keys: Option[BaseIntVector]

    def values: ValueVector

    protected val isDict: Boolean = keys.nonEmpty

    protected val k: BaseIntVector = keys.orNull

    def id(rowId: Int): Int = if (isDict) k.getValueAsLong(rowId).toInt else rowId

    val vector: ValueVector = keys.getOrElse(values)

    def isNullAt(rowId: Int): Boolean = {
      if (isDict) isNull(k, rowId) || isNull(values, id(rowId))
      else isNull(values, rowId)
    }

    private def isNull(vector: ValueVector, index: Int): Boolean = vector.isNull(index)

    final def getNullCount: Int = vector.getNullCount

    final def close(): Unit = {
      keys.foreach(_.close())
      if (values != null) {
        values.close()
      }
    }

    def getBoolean(rowId: Int): Boolean = throw new UnsupportedOperationException

    def getByte(rowId: Int): Byte = throw new UnsupportedOperationException

    def getShort(rowId: Int): Short = throw new UnsupportedOperationException

    def getInt(rowId: Int): Int = throw new UnsupportedOperationException

    def getLong(rowId: Int): Long = throw new UnsupportedOperationException

    def getFloat(rowId: Int): Float = throw new UnsupportedOperationException

    def getDouble(rowId: Int): Double = throw new UnsupportedOperationException

    def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = throw new UnsupportedOperationException

    def getUTF8String(rowId: Int): UTF8String = throw new UnsupportedOperationException

    def getBinary(rowId: Int): Array[Byte] = throw new UnsupportedOperationException

    def getArray(rowId: Int): ColumnarArray = throw new UnsupportedOperationException
  }

  private case class BooleanAccessor(keys: Option[BaseIntVector], values: BitVector) extends ArrowVectorAccessor {
    override final def getBoolean(rowId: Int): Boolean = values.get(id(rowId)) == 1

    override final def getInt(rowId: Int): Int = if (getBoolean(rowId)) 1 else 0

    override final def getLong(rowId: Int): Long = getInt(rowId)

    override final def getUTF8String(rowId: Int): UTF8String = UTF8String.fromString(getBoolean(rowId).toString)
  }

  private abstract class BaseLongAccessor extends ArrowVectorAccessor {
    override final def getBoolean(rowId: Int): Boolean = getLong(rowId) != 0L

    override final def getFloat(rowId: Int): Float = getLong(rowId).toFloat

    override final def getDouble(rowId: Int): Double = getLong(rowId).toDouble

    override final def getUTF8String(rowId: Int): UTF8String = UTF8String.fromString(getLong(rowId).toString)
  }

  private case class ByteAccessor(keys: Option[BaseIntVector], values: TinyIntVector) extends BaseLongAccessor {
    override final def getByte(rowId: Int): Byte = values.get(id(rowId))

    override final def getLong(rowId: Int): Long = getByte(rowId)
  }

  private case class ShortAccessor(keys: Option[BaseIntVector], values: SmallIntVector) extends BaseLongAccessor {
    override final def getShort(rowId: Int): Short = values.get(id(rowId))

    override final def getLong(rowId: Int): Long = getShort(rowId)
  }

  private case class IntAccessor(keys: Option[BaseIntVector], values: IntVector) extends BaseLongAccessor {
    override final def getInt(rowId: Int): Int = values.get(id(rowId))

    override final def getLong(rowId: Int): Long = getInt(rowId)
  }

  private case class LongAccessor(keys: Option[BaseIntVector], values: BigIntVector) extends BaseLongAccessor {
    override final def getLong(rowId: Int): Long = values.get(id(rowId))
  }

  private case class UInt1Accessor(keys: Option[BaseIntVector], values: UInt1Vector) extends BaseLongAccessor {
    override final def getLong(rowId: Int): Long = values.get(id(rowId))

    override final def getInt(rowId: Int): Int = values.get(id(rowId))

    override def getShort(rowId: Int): Short = values.get(id(rowId))

    override def getByte(rowId: Int): Byte = values.get(id(rowId))
  }

  private case class UInt2Accessor(keys: Option[BaseIntVector], values: UInt2Vector) extends BaseLongAccessor {
    override final def getLong(rowId: Int): Long = values.get(id(rowId)).toLong

    override final def getInt(rowId: Int): Int = values.get(id(rowId)).toInt

    override def getShort(rowId: Int): Short = values.get(id(rowId)).toShort
  }

  private case class UInt4Accessor(keys: Option[BaseIntVector], values: UInt4Vector) extends BaseLongAccessor {
    override final def getLong(rowId: Int): Long = values.get(id(rowId))

    override final def getInt(rowId: Int): Int = values.get(id(rowId))
  }

  private case class UInt8Accessor(keys: Option[BaseIntVector], values: UInt8Vector) extends BaseLongAccessor {
    override final def getLong(rowId: Int): Long = values.get(id(rowId))

    override final def getBinary(rowId: Int): Array[Byte] = values.getObjectNoOverflow(id(rowId)).toByteArray

    override final def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
      YTsaurusTypes.longToUnsignedDecimal(values.get(id(rowId)))
    }
  }

  private abstract class BaseDoubleAccessor extends ArrowVectorAccessor {
    override final def getLong(rowId: Int): Long = getDouble(rowId).toLong

    override final def getInt(rowId: Int): Int = getDouble(rowId).toInt

    override final def getUTF8String(rowId: Int): UTF8String = UTF8String.fromString(getDouble(rowId).toString)
  }

  private case class FloatAccessor(keys: Option[BaseIntVector], values: Float4Vector) extends BaseDoubleAccessor {
    override final def getFloat(rowId: Int): Float = values.get(id(rowId))

    override final def getDouble(rowId: Int): Double = getFloat(rowId).toDouble
  }

  private case class DoubleAccessor(keys: Option[BaseIntVector], values: Float8Vector) extends BaseDoubleAccessor {
    override final def getDouble(rowId: Int): Double = values.get(id(rowId))
  }

  private case class DecimalAccessor(keys: Option[BaseIntVector], values: DecimalVector) extends ArrowVectorAccessor {
    override final def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
      if (isNullAt(rowId)) {
        null
      } else {
        Decimal.apply(values.getObject(id(rowId)), precision, scale)
      }
    }
  }

  private case class StringAccessor(keys: Option[BaseIntVector], values: VarCharVector) extends ArrowVectorAccessor {
    final private val stringResult = new NullableVarCharHolder

    override final def getUTF8String(rowId: Int): UTF8String = {
      values.get(id(rowId), stringResult)
      if (stringResult.isSet == 0) {
        null
      } else {
        UTF8String.fromAddress(
          null,
          stringResult.buffer.memoryAddress + stringResult.start,
          stringResult.end - stringResult.start
        )
      }
    }
  }

  private case class BinaryAccessor(keys: Option[BaseIntVector], values: VarBinaryVector) extends ArrowVectorAccessor {
    private def getString(rowId: Int): String = new String(getBinary(rowId), StandardCharsets.UTF_8)

    override def getBoolean(rowId: Int): Boolean = getString(rowId).toBoolean

    override final def getLong(rowId: Int): Long = getString(rowId).toLong

    override final def getInt(rowId: Int): Int = getString(rowId).toInt

    override final def getFloat(rowId: Int): Float = getString(rowId).toFloat

    override final def getDouble(rowId: Int): Double = getString(rowId).toDouble

    override final def getBinary(rowId: Int): Array[Byte] = values.getObject(id(rowId))

    override final def getUTF8String(rowId: Int): UTF8String = {
      if (isDict && k.isNull(rowId)) {
        null
      } else {
        val i = id(rowId)
        val bytes = values.getObject(i)
        if (bytes == null) {
          null
        } else {
          UTF8String.fromBytes(bytes)
        }
      }
    }
  }

  private case class DateAccessor(keys: Option[BaseIntVector], values: DateDayVector) extends ArrowVectorAccessor {
    override final def getInt(rowId: Int): Int = values.get(id(rowId))
  }

  private case class TimestampAccessor(keys: Option[BaseIntVector], values: TimeStampMicroTZVector) extends ArrowVectorAccessor {
    override final def getLong(rowId: Int): Long = values.get(id(rowId))
  }

  private case class ArrayAccessor(keys: Option[BaseIntVector], values: ListVector) extends ArrowVectorAccessor {
    if (keys.nonEmpty) throw new UnsupportedOperationException

    private val dt = dataType.asInstanceOf[IArrayType]
    final private val arrayData = new ArrowColumnVector(dt.element, values.getDataVector, None, false, null)

    override final def isNullAt(rowId: Int): Boolean = { // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (values.getValueCount > 0 && values.getValidityBuffer.capacity == 0) false
      else super.isNullAt(rowId)
    }

    override final def getArray(rowId: Int): ColumnarArray = {
      val offsets = values.getOffsetBuffer
      val index = rowId * BaseRepeatedValueVector.OFFSET_WIDTH
      val start = offsets.getInt(index)
      val end = offsets.getInt(index + BaseRepeatedValueVector.OFFSET_WIDTH)
      new ColumnarArray(arrayData, start, end - start)
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   *
   */
  private case class StructAccessor(keys: Option[BaseIntVector], values: StructVector) extends ArrowVectorAccessor {
    if (keys.nonEmpty) throw new UnsupportedOperationException
  }

  private case class YsonAccessor(keys: Option[BaseIntVector], values: VarBinaryVector) extends ArrowVectorAccessor {
    override def getBinary(rowId: Int): Array[Byte] = {
      values.getObject(id(rowId))
    }

    private def getImpl[T](rowId: Int, sparkType: DataType): T = {
      YsonDecoder.decode(getBinary(rowId), IAtomicType(sparkType)).asInstanceOf[T]
    }

    override def getBoolean(rowId: Int): Boolean = getImpl(rowId, BooleanType)
    override def getFloat(rowId: Int): Float = getDouble(rowId).toFloat
    override def getDouble(rowId: Int): Double = getImpl(rowId, DoubleType)
    override def getUTF8String(rowId: Int): UTF8String = getImpl(rowId, StringType)
    override def getByte(rowId: Int): Byte = getLong(rowId).toByte
    override def getShort(rowId: Int): Short = getLong(rowId).toShort
    override def getInt(rowId: Int): Int = getLong(rowId).toInt
    override def getLong(rowId: Int): Long = getImpl(rowId, LongType)

    override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
      val binaryRepr = getBinary(rowId)
      binaryRepr.head match {
        case YsonTags.BINARY_UINT => getImpl[Decimal](rowId, YTsaurusTypes.UINT64_DEC_TYPE)
        case _ => Decimal(BigDecimal(binaryToText(getBinary(rowId), precision, scale)), precision, scale)
      }
    }
  }

  private case object NullAccessor extends ArrowVectorAccessor {
    override def keys: Option[BaseIntVector] = None

    override def values: ValueVector = null

    override val vector: ValueVector = null

    override def isNullAt(rowId: Int): Boolean = true

    override def getBoolean(rowId: Int): Boolean = null.asInstanceOf[Boolean]

    override def getByte(rowId: Int): Byte = null.asInstanceOf[Byte]

    override def getShort(rowId: Int): Short = null.asInstanceOf[Short]

    override def getInt(rowId: Int): Int = null.asInstanceOf[Int]

    override def getLong(rowId: Int): Long = null.asInstanceOf[Long]

    override def getFloat(rowId: Int): Float = null.asInstanceOf[Float]

    override def getDouble(rowId: Int): Double = null.asInstanceOf[Double]

    override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = null.asInstanceOf[Decimal]

    override def getUTF8String(rowId: Int): UTF8String = null.asInstanceOf[UTF8String]

    override def getBinary(rowId: Int): Array[Byte] = null.asInstanceOf[Array[Byte]]

    override def getArray(rowId: Int): ColumnarArray = null.asInstanceOf[ColumnarArray]
  }

}

object ArrowColumnVector {
  def nullVector(dataType: IndexedDataType): ArrowColumnVector = {
    new ArrowColumnVector(dataType, null, None, isNullVector = true, null)
  }

  def dataType(vector: ValueVector, dictionary: Option[Dictionary]): DataType = {
    val arrowField = dictionary.map(_.getVector.getField).getOrElse(vector.getField)
    ArrowUtils.fromArrowField(arrowField)
  }
}
