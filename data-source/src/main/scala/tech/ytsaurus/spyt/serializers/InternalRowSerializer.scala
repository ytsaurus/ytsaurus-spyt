package tech.ytsaurus.spyt.serializers

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.AsyncWriter
import tech.ytsaurus.client.rows.{WireProtocolWriteable, WireRowSerializer}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.serializers.InternalRowSerializer._
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Unordered, decimalToBinary, stringToBinary}
import tech.ytsaurus.spyt.types.YTsaurusTypes
import tech.ytsaurus.spyt.wrapper.LogLazy
import tech.ytsaurus.typeinfo.TiType

import java.util.concurrent.{Executors, TimeUnit}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class InternalRowSerializer(sparkSchema: StructType,
  writeSchemaConverter: WriteSchemaConverter,
  sortOption: SortOption = Unordered) extends WireRowSerializer[InternalRow] with LogLazy {

  private val log = LoggerFactory.getLogger(getClass)

  private val tableSchema = writeSchemaConverter.tableSchema(sparkSchema, sortOption)
  private val sparkAndTableIndices = {
    val keyIndices = sortOption.keys.map(key => sparkSchema.fieldIndex(key))
    val keyIndicesSet = keyIndices.toSet
    val dataIndices = (0 until sparkSchema.length).filter(i => !keyIndicesSet.contains(i))
    (keyIndices ++ dataIndices).zipWithIndex
  }

  override def getSchema: TableSchema = tableSchema

  private def getColumnType(i: Int): ColumnValueType = {
    def isComposite(t: TiType): Boolean = t.isList || t.isDict || t.isStruct || t.isTuple || t.isVariant

    if (writeSchemaConverter.typeV3Format) {
      val column = tableSchema.getColumnSchema(i)
      val t = column.getTypeV3
      if (t.isOptional) {
        val inner = t.asOptional().getItem
        if (inner.isOptional || isComposite(inner)) {
          ColumnValueType.COMPOSITE
        } else {
          column.getType
        }
      } else if (isComposite(t)) {
        ColumnValueType.COMPOSITE
      } else {
        column.getType
      }
    } else {
      tableSchema.getColumnType(i)
    }
  }

  override def serializeRow(row: InternalRow,
                            writeable: WireProtocolWriteable,
                            keyFieldsOnly: Boolean,
                            aggregate: Boolean,
                            idMapping: Array[Int]): Unit = {
    writeable.writeValueCount(row.numFields)
    sparkAndTableIndices.foreach { case (sparkIndex, tableIndex) =>
      if (row.isNullAt(sparkIndex)) {
        writeable.writeValueHeader(valueId(tableIndex, idMapping), ColumnValueType.NULL, aggregate, 0)
      } else {
        val sparkField = sparkSchema(sparkIndex)
        val ytFieldHint = if (writeSchemaConverter.typeV3Format) {
          Some(tableSchema.getColumnSchema(tableIndex).getTypeV3)
        } else {
          None
        }
        sparkField.dataType match {
          case BinaryType =>
            writeBytes(writeable, idMapping, aggregate, tableIndex, row.getBinary(sparkIndex), getColumnType)
          case StringType =>
            val binary = stringToBinary(ytFieldHint, row.getUTF8String(sparkIndex))
            writeBytes(writeable, idMapping, aggregate, tableIndex, binary, getColumnType)
          case d: DecimalType =>
            val value = row.getDecimal(sparkIndex, d.precision, d.scale)
            if (writeSchemaConverter.typeV3Format) {
              val binary = decimalToBinary(ytFieldHint, d, value)
              writeBytes(writeable, idMapping, aggregate, tableIndex, binary, getColumnType)
            } else {
              val targetColumnType = getColumnType(tableIndex)
              targetColumnType match {
                case ColumnValueType.INT64 | ColumnValueType.UINT64 | ColumnValueType.DOUBLE | ColumnValueType.STRING =>
                  writeHeader(writeable, idMapping, aggregate, tableIndex, 0, _ => targetColumnType)
                  (targetColumnType: @unchecked) match {
                    case ColumnValueType.INT64 | ColumnValueType.UINT64 =>
                      writeable.onInteger(value.toLong)
                    case ColumnValueType.DOUBLE =>
                      writeable.onDouble(value.toDouble)
                    case ColumnValueType.STRING =>
                      writeable.onBytes(value.toString().getBytes)
                  }
                case _ =>
                  throw new IllegalArgumentException("Writing decimal type without enabled type_v3 is not supported")
              }
            }
          case t@(ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
            val skipNulls = sparkField.metadata.contains("skipNulls") && sparkField.metadata.getBoolean("skipNulls")
            writeBytes(writeable, idMapping, aggregate, tableIndex,
              YsonEncoder.encode(row.get(sparkIndex, sparkField.dataType), t, skipNulls, writeSchemaConverter.typeV3Format, ytFieldHint),
              getColumnType)
          case otherType =>
            val isExtendedType = YTsaurusTypes
              .instance
              .wireWriteRow(otherType, row, writeable, aggregate, idMapping, tableIndex, sparkIndex, getColumnType)
            if (!isExtendedType) {
              writeHeader(writeable, idMapping, aggregate, tableIndex, 0, getColumnType)
              otherType match {
                case ByteType => writeable.onInteger(row.getByte(sparkIndex))
                case ShortType => writeable.onInteger(row.getShort(sparkIndex))
                case IntegerType => writeable.onInteger(row.getInt(sparkIndex))
                case LongType => writeable.onInteger(row.getLong(sparkIndex))
                case BooleanType => writeable.onBoolean(row.getBoolean(sparkIndex))
                case FloatType => writeable.onDouble(row.getFloat(sparkIndex))
                case DoubleType => writeable.onDouble(row.getDouble(sparkIndex))
                case DateType => writeable.onInteger(row.getLong(sparkIndex))
                case _: DatetimeType => writeable.onInteger(row.getLong(sparkIndex))
                case TimestampType => writeable.onInteger(row.getLong(sparkIndex))
                case _: Date32Type => writeable.onInteger(row.getInt(sparkIndex))
                case _: Datetime64Type => writeable.onInteger(row.getLong(sparkIndex))
                case _: Timestamp64Type => writeable.onInteger(row.getLong(sparkIndex))
                case _: Interval64Type => writeable.onInteger(row.getLong(sparkIndex))
              }
            }
        }
      }
    }
  }
}

object InternalRowSerializer {

  private def valueId(id: Int, idMapping: Array[Int]): Int = {
    if (idMapping != null) {
      idMapping(id)
    } else id
  }

  def writeHeader(writeable: WireProtocolWriteable, idMapping: Array[Int], aggregate: Boolean,
    tableIndex: Int, length: Int, getColumnType: Int => ColumnValueType): Unit = {
    writeable.writeValueHeader(valueId(tableIndex, idMapping), getColumnType(tableIndex), aggregate, length)
  }

  def writeBytes(writeable: WireProtocolWriteable, idMapping: Array[Int], aggregate: Boolean,
    tableIndex: Int, bytes: Array[Byte], getColumnType: Int => ColumnValueType): Unit = {
    writeHeader(writeable, idMapping, aggregate, tableIndex, bytes.length, getColumnType)
    writeable.onBytes(bytes)
  }
}


