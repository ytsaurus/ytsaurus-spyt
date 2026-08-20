package org.apache.spark.deploy

import org.apache.spark.SparkConf

object YTsaurusShuffleSupport {

  val YTSAURUS_SHUFFLE_ENABLED = "spark.ytsaurus.shuffle.enabled"

  val YTSAURUS_SHUFFLE_CONFS: Map[String, String] = Map(
    "spark.shuffle.manager" -> "org.apache.spark.shuffle.ytsaurus.YTsaurusShuffleManager",
    "spark.shuffle.sort.io.plugin.class" -> "tech.ytsaurus.spyt.shuffle.YTsaurusShuffleDataIO"
  )

  def isYtShuffleEnabled(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean(YTSAURUS_SHUFFLE_ENABLED, false)
  }

  def isYtShuffleEnabled(properties: Map[String, String]): Boolean = {
    properties.get(YTSAURUS_SHUFFLE_ENABLED).exists(_.equalsIgnoreCase("true"))
  }

  def wireYtShuffleConfs(sparkConf: SparkConf): Unit = {
    YTSAURUS_SHUFFLE_CONFS.foreach { case (key, value) =>
      sparkConf.set(key, value)
    }
  }

  def withYtShuffleConfs(properties: Map[String, String]): Map[String, String] = {
    require(properties != null, "sparkProperties must be set")
    if (isYtShuffleEnabled(properties)) {
      properties ++ YTSAURUS_SHUFFLE_CONFS
    } else {
      properties
    }
  }
}
