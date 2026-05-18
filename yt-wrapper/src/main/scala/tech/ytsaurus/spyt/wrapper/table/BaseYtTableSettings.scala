package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.ysontree.YTreeNode

import java.util.{Map => JMap}

class BaseYtTableSettings(schema: TableSchema, rawOptions: JMap[String, Any] = JMap.of()) extends YtTableSettings {
  override def ytSchema: YTreeNode = schema.toYTree

  override def optionsAny: JMap[String, Any] = rawOptions
}
