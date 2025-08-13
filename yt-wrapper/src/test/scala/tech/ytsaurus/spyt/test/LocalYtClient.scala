package tech.ytsaurus.spyt.test

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, TestSuite}
import tech.ytsaurus.spyt.wrapper.client._
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import scala.concurrent.duration._
import scala.language.postfixOps

trait LocalYt extends BeforeAndAfterAll {
  self: TestSuite =>

  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  protected def ytRpcClient: YtRpcClient

  protected implicit lazy val yt: CompoundClient = ytRpcClient.yt
}

object LocalYt {
  val host: String = sys.env.getOrElse("YT_LOCAL_HOST", "localhost")
  val proxyPort = 8000
  val rpcProxyPort = 8002

  val proxy = s"${LocalYt.host}:${LocalYt.proxyPort}"
}

trait LocalYtClient extends LocalYt {
  self: TestSuite =>

  private val conf: YtClientConfiguration = YtClientConfiguration(
    proxy = LocalYt.proxy,
    user = "root",
    token = "",
    timeout = 5 minutes,
    proxyRole = None,
    byop = ByopConfiguration(
      enabled = false,
      ByopRemoteConfiguration(enabled = false, EmptyWorkersListStrategy.Default)
    ),
    masterWrapperUrl = None,
    extendedFileTimeout = true,
    proxyNetworkName = None
  )

  protected val fsConf: Configuration = {
    val c = new Configuration()
    c.set("yt.proxy", s"${LocalYt.host}:${LocalYt.proxyPort}")
    c.set("yt.user", "root")
    c.set("yt.token", "")
    c
  }

  override protected def ytRpcClient: YtRpcClient = {
    YtClientProvider.ytRpcClient(conf)
  }
}
