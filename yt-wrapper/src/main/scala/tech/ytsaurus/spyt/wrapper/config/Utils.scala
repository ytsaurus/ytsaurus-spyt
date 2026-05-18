package tech.ytsaurus.spyt.wrapper.config

import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.utils.CollectionUtils
import tech.ytsaurus.ysontree.YTreeNode

import java.util.{Map => JMap}

object Utils {
  def parseRemoteConfig(path: String, yt: CompoundClient, key: String = "spark_conf"): JMap[String, String] = {
    val remoteConfig = YtWrapper.readDocument(path)(yt).asMap()
    if (remoteConfig.containsKey(key)) {
      CollectionUtils.mapValues(remoteConfig.get(key).asMap(), (node: YTreeNode) => node.stringValue())
    } else {
      JMap.of()
    }
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
