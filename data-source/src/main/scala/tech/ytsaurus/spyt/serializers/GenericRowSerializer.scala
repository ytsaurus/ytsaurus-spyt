package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue, WireProtocolWriter}
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered
import tech.ytsaurus.spyt.types.YTsaurusTypes

import java.util.Base64

// TODO(alex-shishkin): Supported type v1 only
class GenericRowSerializer(schema: StructType) {
  private val tableSchema = new WriteSchemaConverter().tableSchema(schema, Unordered)

  private def boxValue(i: Int, value: Any): UnversionedValue = {
    new UnversionedValue(i, tableSchema.getColumnType(i), false, value)
  }

  def serializeValue(row: Row, i: Int): UnversionedValue = {
    if (row.isNullAt(i)) {
      new UnversionedValue(i, ColumnValueType.NULL, false, null)
    } else {
      val sparkField = schema(i)
      sparkField.dataType match {
        case BinaryType => boxValue(i, row.getAs[Array[Byte]](i))

        case StringType => boxValue(i, row.getString(i).getBytes)
        case t@(ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
          val skipNulls = sparkField.metadata.contains("skipNulls") && sparkField.metadata.getBoolean("skipNulls")
          boxValue(i, YsonEncoder.encode(row.get(i), t, skipNulls))
        case ByteType => boxValue(i, row.getByte(i).toLong)
        case ShortType => boxValue(i, row.getShort(i).toLong)
        case IntegerType => boxValue(i, row.getInt(i).toLong)
        case LongType => boxValue(i, row.getLong(i))
        case BooleanType => boxValue(i, row.getBoolean(i))
        case FloatType => boxValue(i, row.getFloat(i).toDouble)
        case DoubleType => boxValue(i, row.getDouble(i))
        case DateType => boxValue(i, row.getLong(i))
        case _: DatetimeType => boxValue(i, row.getLong(i))
        case TimestampType => boxValue(i, row.getLong(i))
        case _: Date32Type => boxValue(i, row.getInt(i))
        case _: Datetime64Type => boxValue(i, row.getLong(i))
        case _: Timestamp64Type => boxValue(i, row.getLong(i))
        case _: Interval64Type => boxValue(i, row.getLong(i))
        case otherType => YTsaurusTypes.instance.serializeValue(otherType, row, i, boxValue)
      }
    }
  }

  def serializeRow(row: Row): UnversionedRow = {
    import scala.collection.JavaConverters._
    new UnversionedRow((0 until row.length).map(i => serializeValue(row, i)).toList.asJava)
  }

  def serializeTable(rows: Array[Row]): Seq[Array[Byte]] = {
    import scala.collection.JavaConverters._
    val writer = new WireProtocolWriter
    writer.writeTableSchema(tableSchema)
    writer.writeSchemafulRowset(rows.map(serializeRow).toList.asJava)
    val result = writer.finish
    result.asScala
  }
}

object GenericRowSerializer {
  def dfToYTFormatWithBase64(df: DataFrame): Seq[String] = dfToYTFormat(df).map(Base64.getEncoder.encodeToString)

  def dfToYTFormat(df: DataFrame): Seq[Array[Byte]] = new GenericRowSerializer(df.schema).serializeTable(df.collect())
}
