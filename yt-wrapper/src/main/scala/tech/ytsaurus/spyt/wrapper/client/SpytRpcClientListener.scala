package tech.ytsaurus.spyt.wrapper.client

import tech.ytsaurus.client.rpc.RpcClientListener

trait SpytRpcClientListener extends RpcClientListener {
  val id: String
}
