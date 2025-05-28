package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.OptionalConfigEntry
import tech.ytsaurus.ysontree.{YTreeNode, YTreeTextSerializer}

object ShuffleUtils {

  def parseConfig(conf: SparkConf, entry: OptionalConfigEntry[String]): Option[YTreeNode] = {
    conf.get(entry).map(YTreeTextSerializer.deserialize)
  }

}
