package tech.ytsaurus.spyt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.FILES_MAX_PARTITION_BYTES
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.config.Utils.{parseRemoteConfig, remoteGlobalConfigPath, remoteVersionConfigPath}

import java.util.{Map => JMap}

object SessionUtils {
  private val log = LoggerFactory.getLogger(getClass)

  implicit class RichSparkConf(conf: SparkConf) {
    def setEnablers(enablers: JMap[String, String]): SparkConf = {
      enablers.entrySet().stream().forEach { entry =>
        if (conf.contains(entry.getKey)) {
          conf.set(entry.getKey, (conf.get(entry.getKey).toBoolean && entry.getValue.toBoolean).toString)
        }
      }
      conf
    }

    def setAllNoOverride(settings: JMap[String, String]): SparkConf = {
      settings.entrySet().stream().forEach { entry =>
        if (!conf.contains(entry.getKey)) {
          conf.set(entry.getKey, entry.getValue)
        }
      }
      conf
    }
  }

  private[ytsaurus] def mergeConfs(
    conf: SparkConf,
    remoteGlobalConfig: JMap[String, String],
    remoteVersionConfig: JMap[String, String],
    remoteClusterConfig: JMap[String, String],
    enablerMap: JMap[String, String]): SparkConf = {
    conf
      .setAllNoOverride(remoteClusterConfig)
      .setAllNoOverride(remoteVersionConfig)
      .setAllNoOverride(remoteGlobalConfig)
      .setEnablers(enablerMap)
  }

  def prepareSparkConf(): SparkConf = {
    val conf = new SparkConf()
    val sparkClusterVersion = conf.get("spark.yt.cluster.version")
    val sparkClusterConfPath = conf.getOption("spark.yt.cluster.confPath")
    val yt = YtClientProvider.ytClient(ytClientConfiguration(conf))
    val remoteGlobalConfig = parseRemoteConfig(remoteGlobalConfigPath, yt)
    val remoteVersionConfig = parseRemoteConfig(remoteVersionConfigPath(sparkClusterVersion), yt)
    val remoteClusterConfig = sparkClusterConfPath.map(parseRemoteConfig(_, yt)).getOrElse(JMap.of())
    val enablerMap = parseRemoteConfig(remoteVersionConfigPath(sparkClusterVersion), yt, "enablers")
    mergeConfs(conf, remoteGlobalConfig, remoteVersionConfig, remoteClusterConfig, enablerMap)
  }

  //TODO: rethink and refactor due to submission to YT scheduler
  def buildSparkSession(sparkConf: SparkConf): SparkSession = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    log.info(s"SPYT Cluster version: ${sparkConf.get("spark.yt.cluster.version")}")
    log.info(s"SPYT library version: ${sparkConf.get("spark.yt.version")}")
    spark
  }
}
