package tech.ytsaurus.spyt.format

import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Unordered}
import tech.ytsaurus.spyt.serializers.{WriteSchemaConverter, YtLogicalType}
import tech.ytsaurus.spyt.utils.CollectionUtils
import tech.ytsaurus.spyt.wrapper.table.YtTableSettings
import tech.ytsaurus.ysontree.YTreeNode

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

case class TestTableSettings(schema: StructType,
                             isDynamic: Boolean = false,
                             sortOption: SortOption = Unordered,
                             writeSchemaHint: Map[String, YtLogicalType] = Map.empty,
                             otherOptions: Map[String, String] = Map.empty) extends YtTableSettings {
  override def ytSchema: YTreeNode = new WriteSchemaConverter().ytLogicalSchema(schema, sortOption)

  override def optionsAny: JMap[String, Any] = {
    CollectionUtils.concatMaps(otherOptions.asJava, JMap.of("dynamic", isDynamic))
  }
}