package tech.ytsaurus.spyt.serializers

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.TableWriter
import tech.ytsaurus.client.rows.{WireProtocolWriteable, WireRowSerializer}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.serializers.InternalRowSerializer._
import tech.ytsaurus.spyt.serializers.SchemaConverter.{Unordered, decimalToBinary}
import tech.ytsaurus.spyt.types.YTsaurusTypes
import tech.ytsaurus.spyt.wrapper.LogLazy
import tech.ytsaurus.typeinfo.TiType

import java.util.concurrent.{Executors, TimeUnit}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class InternalRowSerializer(schema: StructType, schemaHint: Map[String, YtLogicalType],
                            typeV3Format: Boolean = false) extends WireRowSerializer[InternalRow] with LogLazy {

  private val log = LoggerFactory.getLogger(getClass)

  private val tableSchema = SchemaConverter.tableSchema(schema, Unordered, schemaHint, typeV3Format)

  override def getSchema: TableSchema = tableSchema

  private def getColumnType(i: Int): ColumnValueType = {
    def isComposite(t: TiType): Boolean = t.isList || t.isDict || t.isStruct || t.isTuple || t.isVariant

    if (typeV3Format) {
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
    for {
      i <- 0 until row.numFields
    } {
      if (row.isNullAt(i)) {
        writeable.writeValueHeader(valueId(i, idMapping), ColumnValueType.NULL, aggregate, 0)
      } else {
        val sparkField = schema(i)
        val ytFieldHint = if (typeV3Format) Some(tableSchema.getColumnSchema(i).getTypeV3) else None
        sparkField.dataType match {
          case BinaryType =>
            writeBytes(writeable, idMapping, aggregate, i, row.getBinary(i), getColumnType)
          case StringType =>
            writeBytes(writeable, idMapping, aggregate, i, row.getUTF8String(i).getBytes, getColumnType)
          case d: DecimalType =>
            val value = row.getDecimal(i, d.precision, d.scale)
            if (typeV3Format) {
              val binary = decimalToBinary(ytFieldHint, d, value)
              writeBytes(writeable, idMapping, aggregate, i, binary, getColumnType)
            } else {
              val targetColumnType = getColumnType(i)
              targetColumnType match {
                case ColumnValueType.INT64 | ColumnValueType.UINT64 | ColumnValueType.DOUBLE | ColumnValueType.STRING =>
                  writeHeader(writeable, idMapping, aggregate, i, 0, _ => targetColumnType)
                  targetColumnType match {
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
            writeBytes(writeable, idMapping, aggregate, i,
              YsonEncoder.encode(row.get(i, sparkField.dataType), t, skipNulls, typeV3Format, ytFieldHint),
              getColumnType)
          case otherType =>
            val isExtendedType = YTsaurusTypes
              .instance
              .wireWriteRow(otherType, row, writeable, aggregate, idMapping, i, getColumnType)
            if (!isExtendedType) {
              writeHeader(writeable, idMapping, aggregate, i, 0, getColumnType)
              otherType match {
                case ByteType => writeable.onInteger(row.getByte(i))
                case ShortType => writeable.onInteger(row.getShort(i))
                case IntegerType => writeable.onInteger(row.getInt(i))
                case LongType => writeable.onInteger(row.getLong(i))
                case BooleanType => writeable.onBoolean(row.getBoolean(i))
                case FloatType => writeable.onDouble(row.getFloat(i))
                case DoubleType => writeable.onDouble(row.getDouble(i))
                case DateType => writeable.onInteger(row.getLong(i))
                case _: DatetimeType => writeable.onInteger(row.getLong(i))
                case TimestampType => writeable.onInteger(row.getLong(i))
                case _: Date32Type => writeable.onInteger(row.getInt(i))
                case _: Datetime64Type => writeable.onInteger(row.getLong(i))
                case _: Timestamp64Type => writeable.onInteger(row.getLong(i))
                case _: Interval64Type => writeable.onInteger(row.getLong(i))
              }
            }
        }
      }
    }
  }
}

object InternalRowSerializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, InternalRowSerializer]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)
  private val context = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

  def getOrCreate(schema: StructType,
                  schemaHint: Map[String, YtLogicalType],
                  filters: Array[Filter] = Array.empty,
                  typeV3Format: Boolean = false): InternalRowSerializer = {
    deserializers.get().getOrElseUpdate(schema, new InternalRowSerializer(schema, schemaHint, typeV3Format))
  }

  final def writeRows(writer: TableWriter[InternalRow],
                      rows: java.util.ArrayList[InternalRow],
                      timeout: Duration): Future[Unit] = {
    Future {
      writeRowsRecursive(writer, rows, timeout)
    }(context)
  }

  @tailrec
  private def writeRowsRecursive(writer: TableWriter[InternalRow],
                                 rows: java.util.ArrayList[InternalRow],
                                 timeout: Duration): Unit = {
    if (!writer.write(rows)) {
      YtMetricsRegister.time(writeReadyEventTime, writeReadyEventTimeSum) {
        writer.readyEvent().get(timeout.toMillis, TimeUnit.MILLISECONDS)
      }
      writeRowsRecursive(writer, rows, timeout)
    }
  }

  private def valueId(id: Int, idMapping: Array[Int]): Int = {
    if (idMapping != null) {
      idMapping(id)
    } else id
  }

  def writeHeader(writeable: WireProtocolWriteable, idMapping: Array[Int], aggregate: Boolean,
                          i: Int, length: Int, getColumnType: Int => ColumnValueType): Unit = {
    writeable.writeValueHeader(valueId(i, idMapping), getColumnType(i), aggregate, length)
  }

  def writeBytes(writeable: WireProtocolWriteable, idMapping: Array[Int], aggregate: Boolean,
                         i: Int, bytes: Array[Byte], getColumnType: Int => ColumnValueType): Unit = {
    writeHeader(writeable, idMapping, aggregate, i, bytes.length, getColumnType)
    writeable.onBytes(bytes)
  }
}


