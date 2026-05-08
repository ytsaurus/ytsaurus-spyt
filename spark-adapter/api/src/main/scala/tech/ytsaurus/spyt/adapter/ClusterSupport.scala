package tech.ytsaurus.spyt.adapter

import org.apache.spark.SparkConf

import java.util.ServiceLoader

trait ClusterSupport {
  val masterEndpointName: String

  def msgRegisterDriverToAppId(driverId: String, appId: String): Any
  def msgUnregisterDriverToAppId(driverId: String): Any
}

object ClusterSupport {
  lazy val instance: ClusterSupport = ServiceLoader.load(classOf[ClusterSupport]).findFirst().get()
}