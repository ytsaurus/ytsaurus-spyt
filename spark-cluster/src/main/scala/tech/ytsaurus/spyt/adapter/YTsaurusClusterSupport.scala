package tech.ytsaurus.spyt.adapter

import org.apache.spark.deploy.master.YtMaster
import tech.ytsaurus.spyt.launcher.DeployMessages.{RegisterDriverToAppId, UnregisterDriverToAppId}

class YTsaurusClusterSupport extends ClusterSupport {

  override val masterEndpointName: String = YtMaster.ENDPOINT_NAME

  override def msgRegisterDriverToAppId(driverId: String, appId: String): Any = {
    RegisterDriverToAppId(driverId, appId)
  }

  override def msgUnregisterDriverToAppId(driverId: String): Any = {
    UnregisterDriverToAppId(driverId)
  }
}
