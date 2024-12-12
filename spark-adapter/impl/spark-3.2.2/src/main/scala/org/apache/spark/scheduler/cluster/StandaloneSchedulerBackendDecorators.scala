package org.apache.spark.scheduler.cluster

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rpc.RpcAddress
import tech.ytsaurus.spyt.adapter.ClusterSupport.{instance => cs}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend")
private[spark] class StandaloneSchedulerBackendDecorators {

  @DecoratedMethod
  def connected(appId: String): Unit = {
    __connected(appId)
    val sc = this.getClass.getDeclaredField("org$apache$spark$scheduler$cluster$StandaloneSchedulerBackend$$sc")
      .get(this).asInstanceOf[SparkContext]
    val masters = this.getClass.getDeclaredField("masters").get(this).asInstanceOf[Array[String]]

    StandaloneSchedulerBackendDecorators.registerDriver(sc, masters, conf, appId)
  }

  def __connected(appId: String): Unit = ???
  protected val conf: SparkConf = ???
}

private[spark] object StandaloneSchedulerBackendDecorators {
  def registerDriver(sc: SparkContext, masters: Array[String], conf: SparkConf, appId: String): Unit = {
    conf.getOption("spark.driverId").foreach { driverId =>
      val msg = cs.msgRegisterDriverToAppId(driverId, appId)
      masters.foreach { masterUrl =>
        val masterAddress = RpcAddress.fromSparkURL(masterUrl)
        sc.env.rpcEnv.setupEndpointRef(masterAddress, cs.masterEndpointName).send(msg)
      }
    }
  }
}