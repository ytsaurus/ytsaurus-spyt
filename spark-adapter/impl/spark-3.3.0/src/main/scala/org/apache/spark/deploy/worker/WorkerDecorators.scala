package org.apache.spark.deploy.worker

import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.rpc.RpcEnv
import tech.ytsaurus.spark.launcher.AddressUtils
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.deploy.worker.Worker")
class WorkerDecorators {

  val rpcEnv: RpcEnv = ???
  private var org$apache$spark$deploy$worker$Worker$$webUi: WorkerWebUI = null

  @DecoratedMethod
  def onStart(): Unit = {
    __onStart()
    val webUi = org$apache$spark$deploy$worker$Worker$$webUi
    AddressUtils.writeAddressToFile("worker", rpcEnv.address.host, webUi.boundPort, None, Some(webUi.webUrl), None)
  }

  def __onStart(): Unit = ???

}
