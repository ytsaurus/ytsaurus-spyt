package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.spyt.utils.CollectionUtils.{concatMaps, mapValues}
import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode}

import java.util.{Map => JMap}

trait YtTableSettings {
  def ytSchema: YTreeNode

  def optionsAny: JMap[String, Any]

  def options: JMap[String, YTreeNode] = {
    val optionValues = mapValues(optionsAny, (v: Any) => new YTreeBuilder().value(v).build())
    concatMaps(optionValues, JMap.of("schema", ytSchema))
  }
}
