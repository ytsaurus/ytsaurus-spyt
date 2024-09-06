package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.ysontree.YTreeNode

class BaseYtTableSettings(schema: TableSchema, rawOptions: Map[String, Any] = Map.empty) extends YtTableSettings {
  override def ytSchema: YTreeNode = schema.toYTree

  override def optionsAny: Map[String, Any] = rawOptions
}
