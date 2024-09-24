package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtTable
import tech.ytsaurus.core.common.Decimal.textToBinary
import tech.ytsaurus.spyt.common.utils.TypeUtils.{isTuple, isVariant}
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.isNullTypeAllowed
import tech.ytsaurus.spyt.serialization.IndexedDataType
import tech.ytsaurus.spyt.serialization.IndexedDataType.StructFieldMeta
import tech.ytsaurus.spyt.serializers.YtLogicalType.getStructField
import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.deserializeTypeV3
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTreeNode

object SchemaConverter {
  object MetadataFields {
    val ORIGINAL_NAME = "original_name"
    val KEY_ID = "key_id"
    val TAG = "tag"
    val OPTIONAL = "optional"
    val ARROW_SUPPORTED = "arrow_supported"

    def getOriginalName(field: StructField): String = {
      if (field.metadata.contains(ORIGINAL_NAME)) field.metadata.getString(ORIGINAL_NAME) else field.name
    }

    def isArrowSupported(field: StructField): Boolean = {
      field.metadata.contains(ARROW_SUPPORTED) && field.metadata.getBoolean(ARROW_SUPPORTED)
    }
  }

  sealed trait SortOption {
    def keys: Seq[String]

    def uniqueKeys: Boolean
  }

  case class Sorted(keys: Seq[String], uniqueKeys: Boolean) extends SortOption {
    if (keys.isEmpty) throw new IllegalArgumentException("Sort columns can't be empty in sorted table")
  }

  case object Unordered extends SortOption {
    override def keys: Seq[String] = Nil

    override def uniqueKeys: Boolean = false
  }

  private def getAvailableType(fieldMap: java.util.Map[String, YTreeNode], parsingTypeV3: Boolean): YtLogicalType = {
    if (fieldMap.containsKey("type_v3") && parsingTypeV3) {
      deserializeTypeV3(fieldMap.get("type_v3"))
    } else if (fieldMap.containsKey("type")) {
      val requiredAttribute = fieldMap.get("required")
      val requiredValue = if (requiredAttribute != null) requiredAttribute.boolValue() else false
      wrapSparkAttributes(sparkTypeV1(fieldMap.get("type").stringValue()), !requiredValue)
    } else {
      throw new NoSuchElementException("No parsable data type description")
    }
  }

  def sparkSchema(schemaTree: YTreeNode, schemaHint: Option[StructType] = None, parsingTypeV3: Boolean = true): StructType = {
    import tech.ytsaurus.spyt.wrapper.YtJavaConverters._

    import scala.collection.JavaConverters._
    StructType(schemaTree.asList().asScala.zipWithIndex.map { case (fieldSchema, index) =>
      val fieldMap = fieldSchema.asMap()
      val originalName = fieldMap.getOrThrow("name").stringValue()
      val fieldName = originalName.replace(".", "_")
      val metadata = new MetadataBuilder()
      metadata.putString(MetadataFields.ORIGINAL_NAME, originalName)
      metadata.putLong(MetadataFields.KEY_ID, if (fieldMap.containsKey("sort_order")) index else -1)
      val ytType = getAvailableType(fieldMap, parsingTypeV3)
      metadata.putBoolean(MetadataFields.ARROW_SUPPORTED, ytType.arrowSupported)
      structField(fieldName, ytType, schemaHint, metadata.build())
    })
  }

  def enrichUserProvidedSchema(schema: StructType): StructType = {
    val enrichedFields = schema.fields.map { field =>
      // There may be a bug when user explicitly specifies schema for YTsaurus Interval type, which doesn't support
      // arrow yet, in this case we can suggest to use spark.read.disableArrow option
      if (new WriteSchemaConverter().ytLogicalTypeV3(field.dataType).arrowSupported) {
        val metadataBuilder = new MetadataBuilder()
        metadataBuilder.withMetadata(field.metadata)
        metadataBuilder.putBoolean(MetadataFields.ARROW_SUPPORTED, value = true)
        field.copy(metadata = metadataBuilder.build())
      } else {
        field
      }
    }
    StructType(enrichedFields)
  }

  def prefixKeys(schema: StructType): Seq[String] = {
    keys(schema).takeWhile(_.isDefined).map(_.get)
  }

  def keys(schema: StructType): Seq[Option[String]] = {
    val keyMap = schema
      .fields
      .map(x =>
        if (x.metadata.contains(MetadataFields.KEY_ID))
          (x.metadata.getLong(MetadataFields.KEY_ID), x.metadata.getString(MetadataFields.ORIGINAL_NAME))
        else
          (-1L, x.name)
      )
      .toMap
    val max = if (keyMap.nonEmpty) keyMap.keys.max else -1
    (0L to max).map(keyMap.get)
  }

  def structField(fieldName: String,
                  ytType: YtLogicalType,
                  metadata: Metadata): StructField = {
    getStructField(fieldName, ytType, metadata)
  }

  def structField(fieldName: String,
                  ytType: => YtLogicalType,
                  schemaHint: Option[StructType] = None,
                  metadata: Metadata = Metadata.empty): StructField = {
    schemaHint
      .flatMap(_.find(_.name == fieldName.toLowerCase())
        .map(_.copy(name = fieldName, metadata = metadata))
      )
      .getOrElse(structField(fieldName, ytType, metadata))
  }

  def indexedDataType(dataType: DataType): IndexedDataType = {
    dataType match {
      case s@StructType(fields) if isTuple(s) =>
        val tupleElementTypes = fields.map(element => indexedDataType(element.dataType))
        IndexedDataType.TupleType(tupleElementTypes, s)
      case s@StructType(fields) if isVariant(s) =>
        val transformedStruct = StructType(fields.map(f => f.copy(name = f.name.substring(2))))
        indexedDataType(transformedStruct) match {
          case t: IndexedDataType.TupleType => IndexedDataType.VariantOverTupleType(t)
          case s: IndexedDataType.StructType => IndexedDataType.VariantOverStructType(s)
        }
      case s@StructType(fields) =>
        IndexedDataType.StructType(
          fields.zipWithIndex.map { case (f, i) =>
            f.name -> StructFieldMeta(i, indexedDataType(f.dataType), isNull = f.nullable)
          }.toMap,
          s
        )
      case a@ArrayType(elementType, _) => IndexedDataType.ArrayType(indexedDataType(elementType), a)
      case m@MapType(keyType, valueType, _) => IndexedDataType.MapType(indexedDataType(keyType), indexedDataType(valueType), m)
      case other => IndexedDataType.AtomicType(other)
    }
  }

  def schemaHint(options: Map[String, String]): Option[StructType] = {
    val fields = options.flatMap { case (key, value) =>
      val name = key.stripSuffix("_hint")
      if (name != key) Some(StructField(name, DataType.fromJson(value))) else None
    }

    if (fields.nonEmpty) {
      Some(StructType(fields.toSeq))
    } else {
      None
    }
  }

  def serializeSchemaHint(schema: StructType): Map[String, String] = {
    schema.map(f => (s"${f.name}_hint", f.dataType.json)).toMap
  }

  def wrapSparkAttributes(
    inner: YtLogicalType, flag: Boolean, metadata: Option[Metadata] = None,
  ): YtLogicalType = {
    def wrapNullable(ytType: YtLogicalType): YtLogicalType = {
      val optionalO = metadata.flatMap { m =>
        if (m.contains(MetadataFields.OPTIONAL)) Some(m.getBoolean(MetadataFields.OPTIONAL)) else None
      }
      if (optionalO.getOrElse(flag)) YtLogicalType.Optional(ytType) else ytType
    }

    def wrapTagged(ytType: YtLogicalType): YtLogicalType = {
      val tagO = metadata.flatMap { m =>
        if (m.contains(MetadataFields.TAG)) Some(m.getString(MetadataFields.TAG)) else None
      }
      tagO.map(t => YtLogicalType.Tagged(ytType, t)).getOrElse(ytType)
    }

    wrapNullable(wrapTagged(inner))
  }

  def sparkTypeV1(sType: String): YtLogicalType = {
    YtLogicalType.fromName(sType)
  }

  def checkSchema(schema: StructType, options: Map[String, String]): Unit = {
    schema.foreach { field =>
      if (!YtTable.supportsDataType(field.dataType)) {
        throw new IllegalArgumentException(
          s"YT data source does not support ${field.dataType.simpleString} data type(column `${field.name}`).")
      }
      if (field.dataType == NullType && !isNullTypeAllowed(options)) {
        throw new IllegalArgumentException(
          s"Writing null data type(column `${field.name}`) is not allowed now, because of uselessness. " +
            s"If you are sure you can enable `null_type_allowed` option.")
      }
    }
  }

  def applyYtLimitToSparkDecimal(dataType: DecimalType): DecimalType = {
    val precision = dataType.precision
    val scale = dataType.scale
    val overflow = precision - 35
    if (overflow > 0) {
      if (scale < overflow) {
        throw new IllegalArgumentException("Precision and scale couldn't be reduced for satisfying yt limitations")
      } else {
        DecimalType(precision - overflow, scale - overflow)
      }
    } else {
      dataType
    }
  }

  def decimalToBinary(ytType: Option[TiType], decimalType: DecimalType, decimalValue: Decimal): Array[Byte] = {
    val (precision, scale) = if (ytType.exists(_.isDecimal)) {
      val decimalHint = ytType.get.asDecimal()
      (decimalHint.getPrecision, decimalHint.getScale)
    } else {
      val dT = if (decimalType.precision > 35) applyYtLimitToSparkDecimal(decimalType) else decimalType
      (dT.precision, dT.scale)
    }
    val result = decimalValue.changePrecision(precision, scale)
    if (!result) {
      throw new IllegalArgumentException("Decimal value couldn't fit in yt limitations (precision <= 35)")
    }
    val binary = textToBinary(decimalValue.toBigDecimal.bigDecimal.toPlainString, precision, scale)
    binary
  }
}
