package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import tech.ytsaurus.client.YTGetters
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.{TiType, TypeName}
import tech.ytsaurus.yson.YsonConsumer
import tech.ytsaurus.ysontree.YTreeBinarySerializer

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.UUID
import scala.annotation.tailrec

sealed trait SparkType {
  def topLevel: DataType

  def innerLevel: DataType
}

case class SingleSparkType(topLevel: DataType) extends SparkType {
  override def innerLevel: DataType = topLevel
}

case class TopInnerSparkTypes(topLevel: DataType, innerLevel: DataType) extends SparkType

sealed trait YtLogicalType {
  def value: Int = columnValueType.getValue

  def columnValueType: ColumnValueType

  def getNameV3(inner: Boolean): String = {
    if (inner) {
      alias.name
    } else {
      tiType.getTypeName.getWireName
    }
  }

  def getName(isColumnType: Boolean): String = {
    if (isColumnType) {
      columnValueType.getName
    } else {
      alias.name
    }
  }

  def tiType: TiType

  def sparkType: SparkType

  def nullable: Boolean = false

  def alias: YtLogicalTypeAlias

  def arrowSupported: Boolean = true

  trait FromList extends YTGetters.FromList[ArrayData] {
    override def getTiType: TiType = tiType

    override def getSize(list: ArrayData): Int = list.numElements()
  }

  def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData]

  trait FromStruct extends YTGetters.FromStruct[InternalRow] {
    override def getTiType: TiType = tiType
  }

  def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow]
}

sealed trait YtLogicalTypeAlias {
  def name: String = aliases.head

  def aliases: Seq[String]
}

sealed abstract class AtomicYtLogicalType(name: String,
                                          override val value: Int,
                                          val columnValueType: ColumnValueType,
                                          val tiType: TiType,
                                          val sparkType: SparkType,
                                          otherAliases: Seq[String],
                                          override val arrowSupported: Boolean)
  extends YtLogicalType with YtLogicalTypeAlias {

  def this(name: String, value: Int, columnValueType: ColumnValueType, tiType: TiType, sparkType: DataType,
           otherAliases: Seq[String] = Seq.empty, arrowSupported: Boolean = true) =
    this(name, value, columnValueType, tiType, SingleSparkType(sparkType), otherAliases, arrowSupported)

  override def alias: YtLogicalTypeAlias = this

  override def aliases: Seq[String] = name +: otherAliases
}

sealed trait CompositeYtLogicalType extends YtLogicalType {
  override def columnValueType: ColumnValueType = ColumnValueType.ANY

  override def getName(isColumnType: Boolean): String = ColumnValueType.ANY.getName
}

sealed abstract class CompositeYtLogicalTypeAlias(name: String,
                                                  otherAliases: Seq[String] = Seq.empty) extends YtLogicalTypeAlias {
  override def aliases: Seq[String] = name +: otherAliases
}

object YtLogicalType {

  import tech.ytsaurus.spyt.types.YTsaurusTypes.instance.sparkTypeFor

  case object Null extends AtomicYtLogicalType("null", 0x02, ColumnValueType.NULL, TiType.nullType(), NullType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToNull[ArrayData] with FromList {
        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onEntity()
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToNull[InternalRow] with FromStruct {
        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onEntity()
      }
  }

  case object Int64 extends AtomicYtLogicalType("int64", 0x03, ColumnValueType.INT64, TiType.int64(), LongType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onInteger(list.getLong(i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onInteger(struct.getLong(ordinal))
      }
  }

  case object Uint64 extends AtomicYtLogicalType("uint64", 0x04, ColumnValueType.UINT64, TiType.uint64(), sparkTypeFor(TiType.uint64())) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] = dataType match {
      case decimalType: DecimalType => new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getDecimal(i, decimalType.precision, decimalType.scale).toLong

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onUnsignedInteger(getLong(list, i))
      }
      case _ => new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onUnsignedInteger(getLong(list, i))
      }
    }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] = dataType match {
      case decimalType: DecimalType => new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getDecimal(ordinal, decimalType.precision, decimalType.scale).toLong

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onUnsignedInteger(getLong(struct))
      }
      case _ => new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onUnsignedInteger(getLong(struct))
      }
    }
  }

  case object Float extends AtomicYtLogicalType(
    "float", 0x05, ColumnValueType.DOUBLE, TiType.floatType(),
    TopInnerSparkTypes(FloatType, DoubleType), Seq.empty, arrowSupported = false,
  ) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToFloat[ArrayData] with FromList {
        override def getFloat(list: ArrayData, i: Int): Float = list.getFloat(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onDouble(getFloat(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToFloat[InternalRow] with FromStruct {
        override def getFloat(struct: InternalRow): Float = struct.getFloat(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onDouble(getFloat(struct))
      }
  }

  case object Double extends AtomicYtLogicalType("double", 0x05, ColumnValueType.DOUBLE, TiType.doubleType(), DoubleType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToDouble[ArrayData] with FromList {
        override def getDouble(list: ArrayData, i: Int): Double = list.getDouble(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onDouble(getDouble(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToDouble[InternalRow] with FromStruct {
        override def getDouble(struct: InternalRow): Double = struct.getDouble(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onDouble(getDouble(struct))
      }
  }

  case object Boolean extends AtomicYtLogicalType("boolean", 0x06, ColumnValueType.BOOLEAN, TiType.bool(), BooleanType, Seq("bool")) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToBoolean[ArrayData] with FromList {
        override def getBoolean(list: ArrayData, i: Int): Boolean = list.getBoolean(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onBoolean(list.getBoolean(i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToBoolean[InternalRow] with FromStruct {
        override def getBoolean(struct: InternalRow): Boolean = struct.getBoolean(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onBoolean(struct.getBoolean(ordinal))
      }
  }

  private def getBytes(byteBuffer: ByteBuffer): scala.Array[Byte] = {
    val bytes = new scala.Array[Byte](byteBuffer.remaining())
    byteBuffer.get(bytes)
    bytes
  }

  case object String extends AtomicYtLogicalType("string", 0x10, ColumnValueType.STRING, TiType.string(), StringType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToString[ArrayData] with FromList {
        override def getString(list: ArrayData, i: Int): ByteBuffer = list.getUTF8String(i).getByteBuffer

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(list, i))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToString[InternalRow] with FromStruct {
        override def getString(struct: InternalRow): ByteBuffer = struct.getUTF8String(ordinal).getByteBuffer

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(struct))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }
  }

  case object Binary extends AtomicYtLogicalType("binary", 0x10, ColumnValueType.STRING, TiType.string(), BinaryType) {
    override def getName(isColumnType: Boolean): String = columnValueType.getName

    override def getNameV3(inner: Boolean): String = {
      if (inner) alias.name else "string"
    }

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToString[ArrayData] with FromList {
        override def getString(list: ArrayData, i: Int): ByteBuffer = ByteBuffer.wrap(list.getBinary(i))

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(list, i))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToString[InternalRow] with FromStruct {
        override def getString(struct: InternalRow): ByteBuffer = ByteBuffer.wrap(struct.getBinary(ordinal))

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(struct))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }
  }

  case object Any extends AtomicYtLogicalType("any", 0x11, ColumnValueType.ANY, TiType.yson(), sparkTypeFor(TiType.yson()), Seq("yson")) {
    override def nullable: Boolean = true

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToYson[ArrayData] with FromList {
        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          YTreeBinarySerializer.deserialize(new ByteArrayInputStream(list.getBinary(i)), ysonConsumer)
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToYson[InternalRow] with FromStruct {
        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          YTreeBinarySerializer.deserialize(new ByteArrayInputStream(struct.getBinary(ordinal)), ysonConsumer)
      }
  }

  case object Int8 extends AtomicYtLogicalType("int8", 0x1000, ColumnValueType.INT64, TiType.int8(), ByteType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToByte[ArrayData] with FromList {
        override def getByte(list: ArrayData, i: Int): Byte = list.getByte(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onInteger(list.getByte(i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToByte[InternalRow] with FromStruct {
        override def getByte(struct: InternalRow): Byte = struct.getByte(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onInteger(struct.getByte(ordinal))
      }
  }

  case object Uint8 extends AtomicYtLogicalType("uint8", 0x1001, ColumnValueType.INT64, TiType.uint8(), ShortType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToByte[ArrayData] with FromList {
        override def getByte(list: ArrayData, i: Int): Byte = list.getShort(i).toByte

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getByte(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToByte[InternalRow] with FromStruct {
        override def getByte(struct: InternalRow): Byte = struct.getShort(ordinal).toByte

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getByte(struct))
      }
  }

  case object Int16 extends AtomicYtLogicalType("int16", 0x1003, ColumnValueType.INT64, TiType.int16(), ShortType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToShort[ArrayData] with FromList {
        override def getShort(list: ArrayData, i: Int): Short = list.getShort(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onInteger(list.getShort(i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToShort[InternalRow] with FromStruct {
        override def getShort(struct: InternalRow): Short = struct.getShort(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(struct.getShort(ordinal))
      }
  }

  case object Uint16 extends AtomicYtLogicalType("uint16", 0x1004, ColumnValueType.INT64, TiType.uint16(), IntegerType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToShort[ArrayData] with FromList {
        override def getShort(list: ArrayData, i: Int): Short = list.getInt(i).toShort

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getShort(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToShort[InternalRow] with FromStruct {
        override def getShort(struct: InternalRow): Short = struct.getInt(ordinal).toShort

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getShort(struct))
      }
  }

  case object Int32 extends AtomicYtLogicalType("int32", 0x1005, ColumnValueType.INT64, TiType.int32(), IntegerType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToInt[ArrayData] with FromList {
        override def getInt(list: ArrayData, i: Int): Int = list.getInt(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(list.getInt(i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToInt[InternalRow] with FromStruct {
        override def getInt(struct: InternalRow): Int = struct.getInt(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(struct.getInt(ordinal))
      }
  }

  case object Uint32 extends AtomicYtLogicalType("uint32", 0x1006, ColumnValueType.INT64, TiType.uint32(), LongType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToInt[ArrayData] with FromList {
        override def getInt(list: ArrayData, i: Int): Int = list.getLong(i).toInt

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getInt(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToInt[InternalRow] with FromStruct {
        override def getInt(struct: InternalRow): Int = struct.getLong(ordinal).toInt

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onUnsignedInteger(getInt(struct))
      }
  }

  case object Utf8 extends AtomicYtLogicalType("utf8", 0x1007, ColumnValueType.STRING, TiType.utf8(), StringType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToString[ArrayData] with FromList {
        override def getString(list: ArrayData, i: Int): ByteBuffer = list.getUTF8String(i).getByteBuffer

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(list.getUTF8String(i).getByteBuffer)
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToString[InternalRow] with FromStruct {
        override def getString(struct: InternalRow): ByteBuffer = struct.getUTF8String(ordinal).getByteBuffer

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(struct.getUTF8String(ordinal).getByteBuffer)
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }
  }

  // Unsupported types are listed here: yt/yt/client/arrow/arrow_row_stream_encoder.cpp
  case object Date extends AtomicYtLogicalType("date", 0x1008, ColumnValueType.UINT64, TiType.date(), DateType, arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToInt[ArrayData] with FromList {
        override def getInt(list: ArrayData, i: Int): Int = list.getInt(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getInt(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToInt[InternalRow] with FromStruct {
        override def getInt(struct: InternalRow): Int = struct.getInt(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getInt(struct))
      }
  }

  case object Datetime extends AtomicYtLogicalType("datetime", 0x1009, ColumnValueType.UINT64, TiType.datetime(), new DatetimeType(), arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(struct))
      }
  }

  case object Timestamp extends AtomicYtLogicalType("timestamp", 0x100a, ColumnValueType.UINT64, TiType.timestamp(), TimestampType, arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(struct))
      }
  }

  case object Interval extends AtomicYtLogicalType("interval", 0x100b, ColumnValueType.INT64, TiType.interval(), LongType, arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(getLong(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(getLong(struct))
      }
  }

  case object Void extends AtomicYtLogicalType("void", 0x100c, ColumnValueType.NULL, TiType.voidType(), NullType) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToNull[ArrayData] with FromList {
        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onEntity()
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToNull[InternalRow] with FromStruct {
        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = ysonConsumer.onEntity()
      }
  }

  case object Date32 extends AtomicYtLogicalType("date32", 0x1010, ColumnValueType.INT64, TiType.date32(), new Date32Type(), arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToInt[ArrayData] with FromList {
        override def getInt(list: ArrayData, i: Int): Int = list.getInt(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getInt(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToInt[InternalRow] with FromStruct {
        override def getInt(struct: InternalRow): Int = struct.getInt(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getInt(struct))
      }
  }

  case object Datetime64 extends AtomicYtLogicalType("datetime64", 0x1011, ColumnValueType.INT64, TiType.datetime64(), new Datetime64Type(), arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(struct))
      }
  }

  case object Timestamp64 extends AtomicYtLogicalType("timestamp64", 0x1012, ColumnValueType.INT64, TiType.timestamp64(), new Timestamp64Type(), arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onUnsignedInteger(getLong(struct))
      }
  }

  case object Interval64 extends AtomicYtLogicalType("interval64", 0x1013, ColumnValueType.INT64, TiType.interval64(), new Interval64Type(), arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToLong[ArrayData] with FromList {
        override def getLong(list: ArrayData, i: Int): Long = list.getLong(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(getLong(list, i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToLong[InternalRow] with FromStruct {
        override def getLong(struct: InternalRow): Long = struct.getLong(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonConsumer.onInteger(getLong(struct))
      }
  }

  case class Decimal(precision: Int, scale: Int, decimalType: DecimalType) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(DecimalType(precision, scale))

    override def alias: CompositeYtLogicalTypeAlias = Decimal

    override def tiType: TiType = TiType.decimal(precision, scale)

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToBigDecimal[ArrayData] with FromList {
        override def getBigDecimal(list: ArrayData, i: Int): java.math.BigDecimal =
          list.getDecimal(i, decimalType.precision, decimalType.scale).toJavaBigDecimal.setScale(scale)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBigDecimal(list, i).unscaledValue().toByteArray
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToBigDecimal[InternalRow] with FromStruct {
        override def getBigDecimal(struct: InternalRow): java.math.BigDecimal =
          struct.getDecimal(ordinal, decimalType.precision, decimalType.scale).toJavaBigDecimal.setScale(scale)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBigDecimal(struct).unscaledValue().toByteArray
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }
  }

  case object Decimal extends CompositeYtLogicalTypeAlias("decimal")

  case class Optional(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def value: Int = inner.value

    override def columnValueType: ColumnValueType = inner.columnValueType

    override def tiType: TiType = TiType.optional(inner.tiType)

    override def sparkType: SparkType = inner.sparkType

    override def nullable: Boolean = true

    override def getName(isColumnType: Boolean): String = inner.getName(isColumnType)

    override def alias: CompositeYtLogicalTypeAlias = Optional

    override def arrowSupported: Boolean = inner.arrowSupported

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToOptional[ArrayData] with FromList {
        private val notEmptyGetter = inner.ytGettersFromList(dataType)

        override def getNotEmptyGetter: YTGetters.FromList[ArrayData] = notEmptyGetter

        override def isEmpty(list: ArrayData, i: Int): Boolean = list.isNullAt(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          if (list.isNullAt(i)) {
            ysonConsumer.onEntity()
          } else if (inner.isInstanceOf[Optional]) {
            ysonConsumer.onBeginList()
            ysonConsumer.onListItem()
            notEmptyGetter.getYson(list, i, ysonConsumer)
            ysonConsumer.onEndList()
          } else {
            notEmptyGetter.getYson(list, i, ysonConsumer)
          }
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToOptional[InternalRow] with FromStruct {
        private val notEmptyGetter = inner.ytGettersFromStruct(dataType, ordinal)

        override def getNotEmptyGetter: YTGetters.FromStruct[InternalRow] = notEmptyGetter

        override def isEmpty(struct: InternalRow): Boolean = struct.isNullAt(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          if (struct.isNullAt(ordinal)) {
            ysonConsumer.onEntity()
          } else if (inner.isInstanceOf[Optional]) {
            ysonConsumer.onBeginList()
            ysonConsumer.onListItem()
            notEmptyGetter.getYson(struct, ysonConsumer)
            ysonConsumer.onEndList()
          } else {
            notEmptyGetter.getYson(struct, ysonConsumer)
          }
        }
      }
  }

  case object Optional extends CompositeYtLogicalTypeAlias(TypeName.Optional.getWireName)

  private def resolveInnerType(sparkType: SparkType): DataType = sparkType match {
    case SingleSparkType(sType) => sType
    case TopInnerSparkTypes(_, innerLevel) => innerLevel
  }

  case class Dict(dictKey: YtLogicalType, dictValue: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(
      MapType(dictKey.sparkType.innerLevel, dictValue.sparkType.innerLevel, dictValue.nullable)
    )

    private def newGetter(dataType: DataType): YTGetters.FromDict[MapData, ArrayData, ArrayData] =
      new YTGetters.FromDict[MapData, ArrayData, ArrayData] {
        private val keyGetter = dictKey.ytGettersFromList(dataType.asInstanceOf[MapType].keyType)
        private val valueGetter = dictValue.ytGettersFromList(dataType.asInstanceOf[MapType].valueType)

        override def getKeyGetter: YTGetters.FromList[ArrayData] = keyGetter

        override def getValueGetter: YTGetters.FromList[ArrayData] = valueGetter

        override def getSize(dict: MapData): Int = dict.numElements()

        override def getKeys(dict: MapData): ArrayData = dict.keyArray()

        override def getValues(dict: MapData): ArrayData = dict.valueArray()

        override def getTiType: TiType = tiType
      }

    def newYsonSerializer(getter: YTGetters.FromDict[MapData, ArrayData, ArrayData]): (MapData, YsonConsumer) => Unit = {
      val keyGetter = getter.getKeyGetter
      val valueGetter = getter.getValueGetter
      (dict, ysonConsumer) => {
        ysonConsumer.onBeginList()
        val keys = dict.keyArray()
        val values = dict.valueArray()
        for (i <- 0 until dict.numElements()) {
          ysonConsumer.onListItem()
          ysonConsumer.onBeginList()
          ysonConsumer.onListItem()
          keyGetter.getYson(keys, i, ysonConsumer)
          ysonConsumer.onListItem()
          valueGetter.getYson(values, i, ysonConsumer)
          ysonConsumer.onEndList()
        }
        ysonConsumer.onEndList()
      }
    }

    override def tiType: TiType = TiType.dict(dictKey.tiType, dictValue.tiType)

    override def alias: CompositeYtLogicalTypeAlias = Dict

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToDict[ArrayData, MapData, ArrayData, ArrayData] with FromList {
        private val getter = newGetter(dataType)
        private val ysonSerializer = newYsonSerializer(getter)

        override def getGetter(): YTGetters.FromDict[MapData, ArrayData, ArrayData] = getter

        override def getDict(list: ArrayData, i: Int): MapData = list.getMap(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          ysonSerializer(list.getMap(i), ysonConsumer)
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToDict[InternalRow, MapData, ArrayData, ArrayData] with FromStruct {
        private val getter = newGetter(dataType)
        private val ysonSerializer = newYsonSerializer(getter)

        override def getGetter(): YTGetters.FromDict[MapData, ArrayData, ArrayData] = getter

        override def getDict(struct: InternalRow): MapData = struct.getMap(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          ysonSerializer(struct.getMap(ordinal), ysonConsumer)
      }
  }

  case object Dict extends CompositeYtLogicalTypeAlias(TypeName.Dict.getWireName)

  case class Array(inner: YtLogicalType) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(ArrayType(inner.sparkType.innerLevel, inner.nullable))

    override def tiType: TiType = TiType.list(inner.tiType)

    override def alias: CompositeYtLogicalTypeAlias = Array

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToList[ArrayData, ArrayData] with FromList {
        val elementGetter: YTGetters.FromList[ArrayData] = inner.ytGettersFromList(dataType.asInstanceOf[ArrayType].elementType)

        override def getElementGetter: YTGetters.FromList[ArrayData] = elementGetter

        override def getList(list: ArrayData, i: Int): ArrayData = list.getArray(i)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          onList(ysonConsumer, elementGetter, list.getArray(i))
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToList[InternalRow, ArrayData] with FromStruct {
        val elementGetter: YTGetters.FromList[ArrayData] = inner.ytGettersFromList(dataType.asInstanceOf[ArrayType].elementType)

        override def getElementGetter: YTGetters.FromList[ArrayData] = elementGetter

        override def getList(struct: InternalRow): ArrayData = struct.getArray(ordinal)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          onList(ysonConsumer, elementGetter, struct.getArray(ordinal))
      }

    private def onList(ysonConsumer: YsonConsumer, elementGetter: YTGetters.FromList[ArrayData], value: ArrayData): Unit = {
      ysonConsumer.onBeginList()
      for (j <- 0 until value.numElements()) {
        ysonConsumer.onListItem()
        elementGetter.getYson(value, j, ysonConsumer)
      }
      ysonConsumer.onEndList()
    }
  }

  case object Array extends CompositeYtLogicalTypeAlias(TypeName.List.getWireName)

  case class Struct(fields: Seq[(String, YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(StructType(fields
      .map { case (name, ytType, meta) => getStructField(name, ytType, meta, topLevel = false) }))

    import scala.collection.JavaConverters._

    override def tiType: TiType = TiType.struct(
      fields.map { case (name, ytType, _) => new Member(name, ytType.tiType) }.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Struct

    def newMembersGetters(dataType: DataType): java.util.List[java.util.Map.Entry[String, YTGetters.FromStruct[InternalRow]]] =
      fields.zip(dataType.asInstanceOf[StructType].fields).zipWithIndex.map { case ((field, structField), i) =>
        java.util.Map.entry(field._1, field._2.ytGettersFromStruct(structField.dataType, i))
      }.asJava

    def yson(
              membersGetters: java.util.List[java.util.Map.Entry[String, YTGetters.FromStruct[InternalRow]]],
              internalRow: InternalRow, ysonConsumer: YsonConsumer,
            ): Unit = {
      ysonConsumer.onBeginList()
      for (i <- 0 until membersGetters.size()) {
        ysonConsumer.onListItem()
        membersGetters.get(i).getValue.getYson(internalRow, ysonConsumer)
      }
      ysonConsumer.onEndList()
    }

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToStruct[ArrayData, InternalRow] with FromList {
        private val membersGetters = newMembersGetters(dataType)

        override def getMembersGetters(): java.util.List[java.util.Map.Entry[String, YTGetters.FromStruct[InternalRow]]] =
          membersGetters

        override def getStruct(list: ArrayData, i: Int): InternalRow = list.getStruct(i, fields.size)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          yson(membersGetters, list.getStruct(i, membersGetters.size()), ysonConsumer)
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToStruct[InternalRow, InternalRow] with FromStruct {
        private val membersGetters = newMembersGetters(dataType)

        override def getMembersGetters(): java.util.List[java.util.Map.Entry[String, YTGetters.FromStruct[InternalRow]]] =
          membersGetters

        override def getStruct(struct: InternalRow): InternalRow = struct.getStruct(ordinal, fields.size)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          yson(membersGetters, struct.getStruct(ordinal, membersGetters.size()), ysonConsumer)
      }
  }

  case object Struct extends CompositeYtLogicalTypeAlias(TypeName.Struct.getWireName)

  case class Tuple(elements: Seq[(YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    private val entries = elements.zipWithIndex.map { case ((ytType, _), index) => (s"_${1 + index}", ytType) }

    override def sparkType: SparkType = SingleSparkType(StructType(elements.zipWithIndex
      .map { case ((ytType, meta), index) => getStructField(s"_${1 + index}", ytType, meta, topLevel = false) }))

    import scala.collection.JavaConverters._

    override def tiType: TiType = TiType.tuple(
      elements.map { case (e, _) => e.tiType }.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Tuple

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToStruct[ArrayData, InternalRow] with FromList {
        private val membersGetters = entries.zip(dataType.asInstanceOf[types.StructType]).zipWithIndex.map {
          case (((name, logicalType), structField), i) =>
            java.util.Map.entry(name, logicalType.ytGettersFromStruct(structField.dataType, i))
        }.asJava

        override def getMembersGetters(): java.util.List[java.util.Map.Entry[String, YTGetters.FromStruct[InternalRow]]] = membersGetters

        override def getStruct(list: ArrayData, i: Int): InternalRow = list.getStruct(i, elements.size)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val value = list.getStruct(i, membersGetters.size())
          ysonConsumer.onBeginList()
          membersGetters.forEach { getter =>
            ysonConsumer.onListItem()
            getter.getValue.getYson(value, ysonConsumer)
          }
          ysonConsumer.onEndList()
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToStruct[InternalRow, InternalRow] with FromStruct {
        private val membersGetters = entries.zip(dataType.asInstanceOf[types.StructType]).zipWithIndex.map {
          case (((name, logicalType), structField), i) =>
            java.util.Map.entry(name, logicalType.ytGettersFromStruct(structField.dataType, i))
        }.asJava

        override def getMembersGetters(): java.util.List[java.util.Map.Entry[String, YTGetters.FromStruct[InternalRow]]] = membersGetters

        override def getStruct(struct: InternalRow): InternalRow = struct.getStruct(ordinal, elements.size)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val value = struct.getStruct(ordinal, membersGetters.size())
          ysonConsumer.onBeginList()
          membersGetters.forEach { getter =>
            ysonConsumer.onListItem()
            getter.getValue.getYson(value, ysonConsumer)
          }
          ysonConsumer.onEndList()
        }
      }
  }

  case object Tuple extends CompositeYtLogicalTypeAlias(TypeName.Tuple.getWireName)

  case class Tagged(inner: YtLogicalType, tag: String) extends CompositeYtLogicalType {
    override def sparkType: SparkType = inner.sparkType

    override def tiType: TiType = TiType.tagged(inner.tiType, tag)

    override def alias: CompositeYtLogicalTypeAlias = Tagged

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] = inner.ytGettersFromList(dataType)

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] = inner.ytGettersFromStruct(dataType, ordinal)
  }

  case object Tagged extends CompositeYtLogicalTypeAlias(TypeName.Tagged.getWireName)

  private class VariantGetter(fields: Seq[YtLogicalType], dataType: DataType) {
    private val getters = fields.zip(dataType.asInstanceOf[StructType].fields).zipWithIndex.map {
      case ((field, structField), i) => field.ytGettersFromStruct(structField.dataType, i)
    }

    def get(row: InternalRow, ysonConsumer: YsonConsumer): Unit = {
      val notNulls = (0 until row.numFields).filter(!row.isNullAt(_))
      if (notNulls.isEmpty) {
        throw new IllegalArgumentException("All elements in variant is null")
      } else if (notNulls.size > 1) {
        throw new IllegalArgumentException("Not null element must be single")
      } else {
        val index = notNulls.head
        ysonConsumer.onBeginList()
        ysonConsumer.onListItem()
        ysonConsumer.onInteger(index)
        ysonConsumer.onListItem()
        getters(index).getYson(row, ysonConsumer)
        ysonConsumer.onEndList()
      }
    }
  }

  case class VariantOverStruct(fields: Seq[(String, YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(StructType(fields.map { case (name, ytType, meta) =>
      getStructField(s"_v$name", ytType, meta, forcedNullability = Some(true), topLevel = false)
    }))

    import scala.collection.JavaConverters._

    override def tiType: TiType = TiType.variantOverStruct(
      fields.map { case (name, ytType, _) => new Member(name, ytType.tiType) }.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Variant

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToYson[ArrayData] with FromList {
        val getter = new VariantGetter(fields.map(_._2), dataType)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          getter.get(list.getStruct(i, fields.size), ysonConsumer)
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToYson[InternalRow] with FromStruct {
        val getter = new VariantGetter(fields.map(_._2), dataType)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          getter.get(struct.getStruct(ordinal, fields.size), ysonConsumer)
      }
  }

  case class VariantOverTuple(fields: Seq[(YtLogicalType, Metadata)]) extends CompositeYtLogicalType {
    override def sparkType: SparkType = SingleSparkType(
      StructType(fields.zipWithIndex.map { case ((ytType, meta), index) =>
        getStructField(s"_v_${1 + index}", ytType, meta, forcedNullability = Some(true), topLevel = false)
      })
    )

    import scala.collection.JavaConverters._

    override def tiType: TiType = TiType.variantOverTuple(
      fields.map { case (e, _) => e.tiType }.asJava
    )

    override def alias: CompositeYtLogicalTypeAlias = Variant

    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToYson[ArrayData] with FromList {
        val getter = new VariantGetter(fields.map(_._1), dataType)

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit =
          getter.get(list.getStruct(i, fields.size), ysonConsumer)
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToYson[InternalRow] with FromStruct {
        val getter = new VariantGetter(fields.map(_._1), dataType)

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit =
          getter.get(struct.getStruct(ordinal, fields.size), ysonConsumer)
      }
  }

  case object Uuid extends AtomicYtLogicalType("uuid", 0x100f, ColumnValueType.STRING, TiType.uuid(), StringType, arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToString[ArrayData] with FromList {
        override def getString(struct: ArrayData, i: Int): ByteBuffer = struct.getUTF8String(i).getByteBuffer

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(list, i))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToString[InternalRow] with FromStruct {
        override def getString(struct: InternalRow): ByteBuffer = struct.getUTF8String(ordinal).getByteBuffer

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(struct))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }
  }

  case object Json extends AtomicYtLogicalType("json", 0x100e, ColumnValueType.STRING, TiType.json(), StringType, arrowSupported = false) {
    override def ytGettersFromList(dataType: DataType): YTGetters.FromList[ArrayData] =
      new YTGetters.FromListToString[ArrayData] with FromList {
        override def getString(struct: ArrayData, i: Int): ByteBuffer = struct.getUTF8String(i).getByteBuffer

        override def getYson(list: ArrayData, i: Int, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(list, i))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }

    override def ytGettersFromStruct(dataType: DataType, ordinal: Int): YTGetters.FromStruct[InternalRow] =
      new YTGetters.FromStructToString[InternalRow] with FromStruct {
        override def getString(struct: InternalRow): ByteBuffer = struct.getUTF8String(ordinal).getByteBuffer

        override def getYson(struct: InternalRow, ysonConsumer: YsonConsumer): Unit = {
          val bytes = getBytes(getString(struct))
          ysonConsumer.onString(bytes, 0, bytes.length)
        }
      }
  }

  case object Variant extends CompositeYtLogicalTypeAlias(TypeName.Variant.getWireName)

  private lazy val atomicTypes = Seq(Null, Int64, Uint64, Float, Double, Boolean, String, Binary, Any,
    Int8, Uint8, Int16, Uint16, Int32, Uint32, Utf8, Date, Datetime, Timestamp, Interval, Date32, Datetime64,
    Timestamp64, Interval64, Json, Uuid, Void)

  private lazy val compositeTypes = Seq(Optional, Dict, Array, Struct, Tuple,
    Tagged, Variant, Decimal)

  def fromName(name: String): YtLogicalType = {
    findOrThrow(name, atomicTypes)
  }

  def fromCompositeName(name: String): YtLogicalTypeAlias = {
    findOrThrow(name, compositeTypes)
  }

  private def findOrThrow[T <: YtLogicalTypeAlias](name: String, types: Seq[T]): T = {
    types.find(_.aliases.contains(name))
      .getOrElse(throw new IllegalArgumentException(s"Unknown logical yt type: $name"))
  }

  def getStructField(name: String, ytType: YtLogicalType, metadata: Metadata = Metadata.empty,
                     forcedNullability: Option[Boolean] = None, topLevel: Boolean = true): StructField = {
    val metadataBuilder = new MetadataBuilder
    metadataBuilder.withMetadata(metadata)
    addInnerMetadata(metadataBuilder, ytType)
    forcedNullability.foreach(_ => metadataBuilder.putBoolean(MetadataFields.OPTIONAL, ytType.nullable))
    StructField(
      name,
      if (topLevel) ytType.sparkType.topLevel else ytType.sparkType.innerLevel,
      forcedNullability.getOrElse(ytType.nullable),
      metadataBuilder.build()
    )
  }

  @tailrec
  private def addInnerMetadata(metadataBuilder: MetadataBuilder, ytType: YtLogicalType): Unit = {
    ytType match {
      case o: Optional => addInnerMetadata(metadataBuilder, o.inner)
      case t: Tagged => metadataBuilder.putString(MetadataFields.TAG, t.tag)
      case YtLogicalType.Uuid | YtLogicalType.Json =>
        metadataBuilder.putString(MetadataFields.YT_LOGICAL_TYPE, ytType.getNameV3(true))
      case _ =>
    }
  }
}
