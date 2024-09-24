package tech.ytsaurus.spyt.serializers

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.spyt.types.{Date32Type, Datetime64Type, DatetimeType, Interval64Type, Timestamp64Type}
import org.apache.spark.sql.types._
import tech.ytsaurus.core.tables.{ColumnSortOrder, TableSchema}
import tech.ytsaurus.spyt.common.utils.TypeUtils.{isTuple, isVariant, isVariantOverTuple}
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{StringToUtf8, WriteSchemaHint, WriteTypeV3}
import tech.ytsaurus.spyt.fs.conf.{OptionsConf, SparkYtHadoopConfiguration}
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Unordered, applyYtLimitToSparkDecimal, wrapSparkAttributes}
import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.{serializeType, serializeTypeV3}
import tech.ytsaurus.spyt.types.YTsaurusTypes
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

class WriteSchemaConverter(
  val hint: Map[String, YtLogicalType] = Map.empty,
  val typeV3Format: Boolean = false,
  val stringToUtf8: Boolean = false,
) {
  private def ytLogicalTypeV3Variant(struct: StructType): YtLogicalType = {
    if (isVariantOverTuple(struct)) {
      YtLogicalType.VariantOverTuple {
        struct.fields.map(tF =>
          (wrapSparkAttributes(ytLogicalTypeV3(tF), tF.nullable, Some(tF.metadata)), tF.metadata))
      }
    } else {
      YtLogicalType.VariantOverStruct {
        struct.fields.map(sf => (sf.name.drop(2),
          wrapSparkAttributes(ytLogicalTypeV3(sf), sf.nullable, Some(sf.metadata)), sf.metadata))
      }
    }
  }

  def ytLogicalTypeStruct(structType: StructType): YtLogicalType.Struct = YtLogicalType.Struct {
    structType.fields.map(sf => (sf.name,
      wrapSparkAttributes(ytLogicalTypeV3(sf), sf.nullable, Some(sf.metadata)), sf.metadata))
  }

  def ytLogicalTypeV3(structField: StructField): YtLogicalType =
    ytLogicalTypeV3(structField.dataType, hint.getOrElse(structField.name, null))

  def ytLogicalTypeV3(sparkType: DataType, hint: YtLogicalType = null): YtLogicalType = sparkType match {
    case NullType => YtLogicalType.Null
    case ByteType => YtLogicalType.Int8
    case ShortType => YtLogicalType.Int16
    case IntegerType => YtLogicalType.Int32
    case LongType => YtLogicalType.Int64
    case StringType =>
      if (hint != null) {
        if (hint != YtLogicalType.Utf8 && hint != YtLogicalType.String) {
          throw new IllegalArgumentException(s"casting from $sparkType to $hint is not supported")
        }
        hint
      } else
        if (stringToUtf8) YtLogicalType.Utf8 else YtLogicalType.String
    case FloatType => YtLogicalType.Float
    case DoubleType => YtLogicalType.Double
    case BooleanType => YtLogicalType.Boolean
    case d: DecimalType =>
      val dT = if (d.precision > 35) applyYtLimitToSparkDecimal(d) else d
      YtLogicalType.Decimal(dT.precision, dT.scale)
    case aT: ArrayType =>
      YtLogicalType.Array(wrapSparkAttributes(ytLogicalTypeV3(aT.elementType), aT.containsNull))
    case sT: StructType if isTuple(sT) =>
      YtLogicalType.Tuple {
        sT.fields.map(tF =>
          (wrapSparkAttributes(ytLogicalTypeV3(tF.dataType), tF.nullable, Some(tF.metadata)), tF.metadata))
      }
    case sT: StructType if isVariant(sT) => ytLogicalTypeV3Variant(sT)
    case sT: StructType => ytLogicalTypeStruct(sT)
    case mT: MapType =>
      YtLogicalType.Dict(
        ytLogicalTypeV3(mT.keyType),
        wrapSparkAttributes(ytLogicalTypeV3(mT.valueType), mT.valueContainsNull))
    case BinaryType => YtLogicalType.Binary
    case DateType => YtLogicalType.Date
    case _: DatetimeType => YtLogicalType.Datetime
    case TimestampType => YtLogicalType.Timestamp
    case _: Date32Type => YtLogicalType.Date32
    case _: Datetime64Type => YtLogicalType.Datetime64
    case _: Timestamp64Type => YtLogicalType.Timestamp64
    case _: Interval64Type => YtLogicalType.Interval64
    case otherType => YTsaurusTypes.instance.ytLogicalTypeV3(otherType)
  }

  private def ytLogicalSchemaImpl(sparkSchema: StructType,
                                  sortOption: SortOption,
                                  isTableSchema: Boolean = false): YTreeNode = {
    import scala.collection.JavaConverters._

    def serializeColumn(field: StructField, sort: Boolean): YTreeNode = {
      val builder = YTree.builder
        .beginMap
        .key("name").value(field.name)
      val fieldType = hint.getOrElse(field.name,
        wrapSparkAttributes(ytLogicalTypeV3(field), field.nullable, Some(field.metadata)))
      if (typeV3Format) {
        builder
          .key("type_v3").value(serializeTypeV3(fieldType))
      } else {
        builder
          .key("type").value(serializeType(fieldType, isTableSchema))
          .key("required").value(false)
      }
      if (sort) builder.key("sort_order").value(ColumnSortOrder.ASCENDING.getName)
      builder.buildMap
    }

    val sortColumnsSet = sortOption.keys.toSet
    val sortedFields =
      sortOption.keys
        .map(sparkSchema.apply)
        .map(f => serializeColumn(f, sort = true)
        ) ++ sparkSchema
        .filter(f => !sortColumnsSet.contains(f.name))
        .map(f => serializeColumn(f, sort = false))

    YTree.builder
      .beginAttributes
      .key("strict").value(true)
      .key("unique_keys").value(sortOption.uniqueKeys)
      .endAttributes
      .value(sortedFields.asJava)
      .build
  }

  def ytLogicalSchema(sparkSchema: StructType, sortOption: SortOption = Unordered): YTreeNode = {
    ytLogicalSchemaImpl(sparkSchema, sortOption)
  }

  def tableSchema(sparkSchema: StructType, sortOption: SortOption = Unordered): TableSchema = {
    TableSchema.fromYTree(ytLogicalSchemaImpl(sparkSchema, sortOption,
      isTableSchema = true))
  }
}

object WriteSchemaConverter {
  def apply(options: Map[String, String]) = new WriteSchemaConverter(
    options.ytConf(WriteSchemaHint),
    options.ytConf(WriteTypeV3),
    options.ytConf(StringToUtf8),
  )

  def apply(configuration: Configuration) = new WriteSchemaConverter(
    configuration.ytConf(WriteSchemaHint),
    configuration.ytConf(WriteTypeV3),
    configuration.ytConf(StringToUtf8),
  )
}
