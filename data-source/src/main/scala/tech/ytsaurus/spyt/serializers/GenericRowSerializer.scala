package tech.ytsaurus.spyt.serializers

import NYT.NTableClient.NProto.ChunkMeta.TLogicalType._
import NYT.NTableClient.NProto.ChunkMeta.{TColumnSchema, TLogicalType, TTableSchemaExt}
import org.apache.spark.sql.spyt.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue, WireProtocolWriter}
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.types.YTsaurusTypes

import java.nio.charset.StandardCharsets
import java.util.Base64

class GenericRowSerializer(schema: StructType) {
  private val converter = new WriteSchemaConverter(typeV3Format = true)

  private def toYtLogicalType(field: StructField): YtLogicalType = {
    converter.ytLogicalTypeV3(field)
  }

  private def boxValue(i: Int, value: Any): UnversionedValue = {
    new UnversionedValue(i, toYtLogicalType(schema(i)).columnValueType, false, value)
  }

  def serializeValue(row: Row, i: Int): UnversionedValue = {
    if (row.isNullAt(i)) {
      new UnversionedValue(i, ColumnValueType.NULL, false, null)
    } else {
      val sparkField = schema(i)
      val skipNulls = sparkField.metadata.contains("skipNulls") && sparkField.metadata.getBoolean("skipNulls")
      sparkField.dataType match {
        case BinaryType => boxValue(i, row.getAs[Array[Byte]](i))

        case StringType => boxValue(i, row.getString(i).getBytes(StandardCharsets.UTF_8))
        case t@(ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
          boxValue(i, YsonEncoder.encode(row.get(i), t, skipNulls, typeV3Format = true, Some(toYtLogicalType(sparkField).tiType)))
        case ByteType => boxValue(i, row.getByte(i).toLong)
        case ShortType => boxValue(i, row.getShort(i).toLong)
        case IntegerType => boxValue(i, row.getInt(i).toLong)
        case LongType => boxValue(i, row.getLong(i))
        case BooleanType => boxValue(i, row.getBoolean(i))
        case FloatType => boxValue(i, row.getFloat(i).toDouble)
        case DoubleType => boxValue(i, row.getDouble(i))
        case DateType => boxValue(i, row.getDate(i).toLocalDate.toEpochDay)
        case _: DatetimeType => boxValue(i, row.getAs[Datetime](i).toLong)
        // using toInstant.toEpochMilli instead of getTime to preserve milliseconds
        case TimestampType => boxValue(i, row.getTimestamp(i).toInstant.toEpochMilli * 1000L)
        case _: Date32Type => boxValue(i, row.getAs[Date32](i).toInt.toLong)
        case _: Datetime64Type => boxValue(i, row.getAs[Datetime64](i).toLong)
        case _: Timestamp64Type => boxValue(i, row.getAs[Timestamp64](i).toLong)
        case _: Interval64Type => boxValue(i, row.getAs[Interval64](i).toLong)
        case dt: DecimalType => boxValue(i, SchemaConverter.decimalToBinary(None, dt, Decimal(row.getDecimal(i))))
        case otherType => YTsaurusTypes.instance.serializeValue(otherType, row, i, boxValue)
      }
    }
  }

  private def toProtobufLogicalType(ytLogicalType: YtLogicalType): (TLogicalType.Builder, Option[Int]) = {
    val logicalType: TLogicalType.Builder = TLogicalType.newBuilder()
    var simpleType: Option[Int] = None

    def recurse(ytLogicalType: YtLogicalType): TLogicalType.Builder = toProtobufLogicalType(ytLogicalType)._1

    ytLogicalType match {
      case atomicType: AtomicYtLogicalType =>
        simpleType = Some(atomicType.value)
        logicalType.setSimple(atomicType.value)

      case YtLogicalType.Optional(inner) => logicalType.setOptional(recurse(inner))

      case YtLogicalType.Array(inner) => logicalType.setList(recurse(inner))

      case YtLogicalType.Struct(fields) =>
        val structLogicalType = fields.foldLeft(TStructLogicalType.newBuilder()) { case (builder, (name, ytType, _)) =>
          builder.addFields(TStructField.newBuilder().setName(name).setType(recurse(ytType)))
        }
        logicalType.setStruct(structLogicalType)

      case YtLogicalType.Tuple(fields) =>
        val tupleLogicalType = fields.foldLeft(TTupleLogicalType.newBuilder()) { case (builder, (ytType, _)) =>
          builder.addElements(recurse(ytType))
        }
        logicalType.setTuple(tupleLogicalType)

      case YtLogicalType.VariantOverTuple(fields) =>
        val variantTupleLT = fields.foldLeft(TVariantTupleLogicalType.newBuilder()) { case (builder, (ytType, _)) =>
          builder.addElements(recurse(ytType))
        }
        logicalType.setVariantTuple(variantTupleLT)

      case YtLogicalType.VariantOverStruct(fields) =>
        val variantStructLT = fields.foldLeft(TVariantStructLogicalType.newBuilder()) { case (builder, (name, ytType, _)) =>
          builder.addFields(TStructField.newBuilder().setName(name).setType(recurse(ytType)))
        }
        logicalType.setVariantStruct(variantStructLT)

      case YtLogicalType.Dict(keyType, valueType) => logicalType.setDict(
        TDictLogicalType.newBuilder().setKey(recurse(keyType)).setValue(recurse(valueType))
      )

      case YtLogicalType.Tagged(inner, tag) => logicalType.setTagged(
        TTaggedLogicalType.newBuilder().setElement(recurse(inner)).setTag(tag)
      )

      case YtLogicalType.Decimal(precision, scale) => logicalType.setDecimal(
        TDecimalLogicalType.newBuilder().setPrecision(precision).setScale(scale)
      )
    }
    (logicalType, simpleType)
  }

  private def serializeTableSchemaExt(): TTableSchemaExt = {
    val schemaBuilder = TTableSchemaExt.newBuilder()
    schemaBuilder.setStrict(true)
    schemaBuilder.setUniqueKeys(false)

    schema.fields.foreach { field =>
      val columnBuilder = TColumnSchema.newBuilder()
      val ytLogicalType = toYtLogicalType(field)
      val required = !field.nullable
      columnBuilder.setName(field.name)
      columnBuilder.setType(ytLogicalType.columnValueType.getValue)
      columnBuilder.setRequired(required)

      var (logicalType, simpleTypeOpt) = toProtobufLogicalType(ytLogicalType)

      simpleTypeOpt.foreach(simpleType => columnBuilder.setSimpleLogicalType(simpleType))

      if (!required) {
        logicalType = TLogicalType.newBuilder().setOptional(logicalType)
      }

      columnBuilder.setLogicalType(logicalType)

      schemaBuilder.addColumns(columnBuilder)
    }

    schemaBuilder.build()
  }

  def serializeRow(row: Row): UnversionedRow = {
    import scala.collection.JavaConverters._
    new UnversionedRow((0 until row.length).map(i => serializeValue(row, i)).toList.asJava)
  }

  def serializeTable(rows: Array[Row]): Seq[Array[Byte]] = {
    import scala.collection.JavaConverters._
    val writer = new WireProtocolWriter
    writer.writeMessage(serializeTableSchemaExt())
    writer.writeSchemafulRowset(rows.map(serializeRow).toList.asJava)
    val result = writer.finish
    result.asScala
  }
}

object GenericRowSerializer {

  // COMPAT(atokarew):
  @deprecated(
    message = "Left here for older query tracker versions, subject to remove later",
    since = "2.6.0"
  )
  def dfToYTFormatWithBase64(df: DataFrame): Seq[String] = {
    dfToYTFormatWithBase64(df, 10000).tail
  }

  def dfToYTFormatWithBase64(df: DataFrame, rowCountLimit: Int): Seq[String] = {
    val result = df.collect()
    val isTruncated = if (result.length > rowCountLimit) "T" else "F"
    val wireResult = new GenericRowSerializer(df.schema).serializeTable(result.take(rowCountLimit))
    Seq(isTruncated) ++ wireResult.map(Base64.getEncoder.encodeToString)
  }
}
