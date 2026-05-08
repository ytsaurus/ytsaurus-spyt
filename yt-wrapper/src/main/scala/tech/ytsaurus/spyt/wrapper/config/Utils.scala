package tech.ytsaurus.spyt.wrapper.config

import tech.ytsaurus.spyt.wrapper.YtJavaConverters.RichJavaMap
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient

import scala.collection.JavaConverters.mapAsScalaMapConverter

object Utils {
  def parseRemoteConfig(path: String, yt: CompoundClient, key: String = "spark_conf"): Map[String, String] = {
    val remoteConfig = YtWrapper.readDocument(path)(yt).asMap().getOption(key)
    remoteConfig.map { config =>
      config.asMap().asScala.toMap.mapValues(_.stringValue())
    }.getOrElse(Map.empty)
  }

  def remoteGlobalConfigPath: String = "//home/spark/conf/global"

  def remoteVersionConfigPath(spytVersion: String): String = {
    val subDir = releaseTypeDirectory(spytVersion)
    s"//home/spark/conf/$subDir/$spytVersion/spark-launch-conf"
  }

  def remoteClusterConfigPath(discoveryPath: String): String = s"$discoveryPath/discovery/conf"

  def releaseTypeDirectory(version: String): String = {
    if (version.endsWith("-SNAPSHOT")) {
      "snapshots"
    } else if (Seq("alpha", "beta", "rc").exists(rt => version.contains(rt))) {
      "pre-releases"
    } else {
      "releases"
    }
  }
}
