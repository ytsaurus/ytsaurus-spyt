package tech.ytsaurus.spyt.format.conf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, StructType}
import tech.ytsaurus.spyt.wrapper.config._
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Sorted, Unordered}
import tech.ytsaurus.spyt.serializers.{SchemaConverter, WriteSchemaConverter, YtLogicalType}
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.spyt.wrapper.config.ConfigEntry
import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.{deserializeTypeV3, serializeTypeV3}
import tech.ytsaurus.ysontree.YTreeNode
import tech.ytsaurus.ysontree.YTreeTextSerializer

case class YtTableSparkSettings(configuration: Configuration) extends YtTableSettings {

  import YtTableSparkSettings._

  private def sortOption: SortOption = {
    val keys = configuration.ytConf(SortColumns)
    val uniqueKeys = configuration.ytConf(UniqueKeys)
    if (keys.isEmpty && uniqueKeys) {
      throw new IllegalArgumentException("Unique keys attribute can't be true for unordered table")
    }
    if (keys.nonEmpty) Sorted(keys, uniqueKeys) else Unordered
  }

  private def writeSchemaConverter = WriteSchemaConverter(configuration)

  private def schema: StructType = configuration.ytConf(Schema).sparkType.topLevel.asInstanceOf[StructType]

  override def ytSchema: YTreeNode = writeSchemaConverter.ytLogicalSchema(schema, sortOption)

  override def optionsAny: Map[String, Any] = {
    val optionsKeys = configuration.ytConf(Options)
    optionsKeys.collect { case key if !Options.excludeOptions.contains(key) =>
      key -> Options.get(key, configuration)
    }.toMap
  }
}

object YtTableSparkSettings {
  import ConfigEntry.implicits._
  import ConfigEntry.{fromJsonTyped, toJsonTyped}

  implicit val structTypeAdapter: ValueAdapter[YtLogicalType.Struct] = new ValueAdapter[YtLogicalType.Struct] {
    override def get(value: String): YtLogicalType.Struct =
      deserializeTypeV3(YTreeTextSerializer.deserialize(value)).asInstanceOf[YtLogicalType.Struct]

    override def set(value: YtLogicalType.Struct): String =
      YTreeTextSerializer.serialize(serializeTypeV3(value, innerForm = true))
  }

  implicit val logicalTypeMapAdapter: ValueAdapter[Map[String, YtLogicalType]] = new ValueAdapter[Map[String, YtLogicalType]] {
    override def get(value: String): Map[String, YtLogicalType] = {
      fromJsonTyped[Map[String, String]](value).mapValues(t => deserializeTypeV3(YTreeTextSerializer.deserialize(t)))
    }

    override def set(value: Map[String, YtLogicalType]): String = {
      toJsonTyped[Map[String, String]](value.mapValues(t => YTreeTextSerializer.serialize(serializeTypeV3(t))))
    }
  }
  
  // read
  case object OptimizedForScan extends ConfigEntry[Boolean]("is_scan")

  case object ArrowEnabled extends ConfigEntry[Boolean]("arrow_enabled", Some(true))

  case object KeyPartitioned extends ConfigEntry[Boolean]("key_partitioned")

  case object Dynamic extends ConfigEntry[Boolean]("dynamic")

  case object Transaction extends ConfigEntry[String]("transaction")

  case object Timestamp extends ConfigEntry[Long]("timestamp")

  case object InconsistentReadEnabled extends ConfigEntry[Boolean]("enable_inconsistent_read", Some(false))

  // write
  case object IsTable extends ConfigEntry[Boolean]("is_table", Some(false))

  case object SortColumns extends ConfigEntry[Seq[String]]("sort_columns", Some(Nil))

  case object TableWriterConfig extends ConfigEntry[YTreeNode]("table_writer")

  case object UniqueKeys extends ConfigEntry[Boolean]("unique_keys", Some(false))

  case object InconsistentDynamicWrite extends ConfigEntry[Boolean]("inconsistent_dynamic_write", Some(false))

  case object WriteSchemaHint extends ConfigEntry[Map[String, YtLogicalType]]("write_schema_hint", Some(Map.empty))

  case object Schema extends ConfigEntry[YtLogicalType.Struct]("schema")

  case object WriteTransaction extends ConfigEntry[String]("write_transaction")

  case object WriteTypeV3 extends ConfigEntry[Boolean]("write_type_v3", Some(false))

  case object StringToUtf8 extends ConfigEntry[Boolean]("string_to_utf8", Some(false))

  case object NullTypeAllowed extends ConfigEntry[Boolean]("null_type_allowed", Some(false))

  case object OptimizeFor extends ConfigEntry[String]("optimize_for")

  case object Path extends ConfigEntry[String]("path")

  case object Options extends ConfigEntry[Seq[String]]("options") {

    private val transformOptions: Set[ConfigEntry[_]] = Set(Dynamic)

    def get(key: String, configuration: Configuration): Any = {
      val str = configuration.getYtConf(key).get
      transformOptions.collectFirst {
        case conf if conf.name == key => conf.get(str)
      }.getOrElse(str)
    }

    val excludeOptions: Set[String] = Set(SortColumns, Schema, WriteTypeV3, NullTypeAllowed, Path, TableWriterConfig).map(_.name)
  }

  def isTable(configuration: Configuration): Boolean = {
    configuration.ytConf(IsTable)
  }

  def isNullTypeAllowed(options: Map[String, String]): Boolean = {
    options.ytConf(NullTypeAllowed)
  }

  def isTableSorted(configuration: Configuration): Boolean = {
    configuration.getYtConf(SortColumns).exists(_.nonEmpty)
  }

  def deserialize(configuration: Configuration): YtTableSparkSettings = {
    YtTableSparkSettings(configuration)
  }

  def serialize(options: Map[String, String], schema: YtLogicalType.Struct, configuration: Configuration): Unit = {
    options.foreach { case (key, value) =>
      configuration.setYtConf(key, value)
    }
    configuration.setYtConf(Schema, schema)
    configuration.setYtConf(Options, options.keys.toSeq)
    configuration.setYtConf(IsTable, true)
  }
}
