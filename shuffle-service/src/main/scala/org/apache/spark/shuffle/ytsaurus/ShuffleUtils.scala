package org.apache.spark.shuffle.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ytsaurus.Config.YTSAURUS_SHUFFLE_CONFIG
import tech.ytsaurus.ysontree.{YTreeMapNode, YTreeTextSerializer}

import scala.jdk.CollectionConverters._

object ShuffleUtils {

  private val PULL_SECTION = "pull"
  private val PUSH_SECTION = "push"

  def shuffleConfig(conf: SparkConf, pushBasedEnabled: Boolean): Option[YTreeMapNode] = {
    val config = conf.get(YTSAURUS_SHUFFLE_CONFIG).map(YTreeTextSerializer.deserialize(_).mapNode())
    config.foreach(validateModeSection(_, pushBasedEnabled))
    config
  }

  private def validateModeSection(config: YTreeMapNode, pushBasedEnabled: Boolean): Unit = {
    val activeSection = if (pushBasedEnabled) PUSH_SECTION else PULL_SECTION
    val unexpectedKeys = config.keys().asScala.filterNot(_ == activeSection)
    require(unexpectedKeys.isEmpty,
      s"${YTSAURUS_SHUFFLE_CONFIG.key} may contain only the '$activeSection' section when push-based shuffle is " +
        s"${if (pushBasedEnabled) "enabled" else "disabled"}, got unexpected keys: ${unexpectedKeys.mkString(", ")}")
  }
}
