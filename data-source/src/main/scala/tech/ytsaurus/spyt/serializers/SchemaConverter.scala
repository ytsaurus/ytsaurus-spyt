package tech.ytsaurus.spyt.serializers

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtTable
import org.apache.spark.unsafe.types.UTF8String
import tech.ytsaurus.core.common.Decimal.textToBinary
import tech.ytsaurus.core.tables.ColumnSortOrder
import tech.ytsaurus.spyt.common.utils.TypeUtils.{isTuple, isVariant}
import tech.ytsaurus.spyt.common.utils.UuidUtils
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{SortColumns, SortOrders, UniqueKeys, isNullTypeAllowed}
import tech.ytsaurus.spyt.serialization.IndexedDataType
import tech.ytsaurus.spyt.serialization.IndexedDataType.StructFieldMeta
import tech.ytsaurus.spyt.serializers.YtLogicalType.getStructField
import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.deserializeTypeV3
import tech.ytsaurus.spyt.wrapper.config._
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTreeNode

import scala.annotation.tailrec

object SchemaConverter {
  object MetadataFields {
    val ORIGINAL_NAME = "original_name"
    val KEY_ID = "key_id"
    val TAG = "tag"
    val OPTIONAL = "optional"
    val ARROW_SUPPORTED = "arrow_supported"
    val YT_LOGICAL_TYPE = "yt_logical_type"

    def getOriginalName(field: StructField): String = {
      if (field.metadata.contains(ORIGINAL_NAME)) field.metadata.getString(ORIGINAL_NAME) else field.name
    }

    def isArrowSupported(field: StructField): Boolean = {
      field.metadata.contains(ARROW_SUPPORTED) && field.metadata.getBoolean(ARROW_SUPPORTED)
    }
  }

  sealed trait SortOrder {
    def toColumnSortOrder: ColumnSortOrder
    def toString: String
  }

  object SortOrder {
    case object Asc extends SortOrder {
      override def toColumnSortOrder: ColumnSortOrder = ColumnSortOrder.ASCENDING
      override def toString: String = "asc"
    }

    case object Desc extends SortOrder {
      override def toColumnSortOrder: ColumnSortOrder = ColumnSortOrder.DESCENDING
      override def toString: String = "desc"
    }
  }

  sealed trait SortOption {
    def keys: Seq[String]
    def orders: Seq[SortOrder]
    def uniqueKeys: Boolean
  }

  case class Sorted(keys: Seq[String], orders: Seq[SortOrder], uniqueKeys: Boolean) extends SortOption {
    require(keys.nonEmpty, "Sort columns can't be empty")
    require(keys.length == orders.length, s"Orders count (${orders.length}) must match keys count (${keys.length})")
  }

  object Sorted {
    def apply(keys: Seq[String], uniqueKeys: Boolean): Sorted =
      fromStringOrders(keys, Seq.empty, uniqueKeys)

    def fromStringOrders(keys: Seq[String], orders: Seq[String], uniqueKeys: Boolean): Sorted = {
      val parsedOrders = if (orders.isEmpty) {
        Seq.fill(keys.length)(SortOrder.Asc)
      } else {
        orders.map {
          case "asc" => SortOrder.Asc
          case "desc" => SortOrder.Desc
          case invalid => throw new IllegalArgumentException(s"Invalid sort order param: '$invalid'")
        }
      }
      Sorted(keys, parsedOrders, uniqueKeys)
    }
  }

  case object Unordered extends SortOption {
    override def keys: Seq[String] = Nil
    override def orders: Seq[SortOrder] = Nil
    override def uniqueKeys: Boolean = false
  }

  def getSortOption(configuration: Configuration): SortOption = {
    val keys = configuration.ytConf(SortColumns)
    val uniqueKeys = configuration.ytConf(UniqueKeys)
    val orders = configuration.ytConf(SortOrders)

    if (keys.isEmpty && uniqueKeys) {
      throw new IllegalArgumentException("Unique keys attribute can't be true for unordered table")
    }

    if (keys.nonEmpty) Sorted.fromStringOrders(keys, orders, uniqueKeys) else Unordered
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
      setYtLogicalTypeIfUnsignedType(ytType, metadata)
      metadata.putBoolean(MetadataFields.ARROW_SUPPORTED, ytType.arrowSupported)
      structField(fieldName, ytType, schemaHint, metadata.build())
    })
  }

  @tailrec
  private def setYtLogicalTypeIfUnsignedType(ytType: YtLogicalType, metadata: MetadataBuilder): Unit = {
    ytType match {
      case YtLogicalType.Optional(innerType) => setYtLogicalTypeIfUnsignedType(innerType, metadata)
      case YtLogicalType.Uint8 => metadata.putString(MetadataFields.YT_LOGICAL_TYPE, "uint8")
      case YtLogicalType.Uint16 => metadata.putString(MetadataFields.YT_LOGICAL_TYPE, "uint16")
      case YtLogicalType.Uint32 => metadata.putString(MetadataFields.YT_LOGICAL_TYPE, "uint32")
      case _ => ()
    }
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
        (indexedDataType(transformedStruct): @unchecked) match {
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

  def stringToBinary(ytType: Option[TiType], stringValue: UTF8String): Array[Byte] = {
    ytType match {
      case Some(tiType) if tiType.isUuid => UuidUtils.UTF8UuidToBytes(stringValue)
      case _ => stringValue.getBytes
    }
  }
}
