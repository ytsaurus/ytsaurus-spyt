package tech.ytsaurus.spyt.wrapper.client

import org.mockito.Mockito
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.bus.DefaultBusConnector
import tech.ytsaurus.spyt.test.LocalYt

import scala.collection.concurrent.TrieMap

class YtClientProviderTest extends AnyFlatSpec with Matchers {
  behavior of "YtClientProvider"

  private val conf = YtClientConfiguration.default(LocalYt.proxy, "root", "")

  private def withScopes(scopes: String*)(body: => Unit): Unit = {
    try {
      body
    } finally {
      scopes.foreach(YtClientProvider.closeScope)
    }
  }

  it should "share a scoped client only within its scope" in withScopes("scope-a", "scope-b") {
    val scopedClient = YtClientProvider.scopedYtRpcClient(conf, "scope-a")

    YtClientProvider.scopedYtRpcClient(conf, "scope-a") should be theSameInstanceAs scopedClient
    YtClientProvider.scopedYtRpcClient(conf, "scope-b") shouldNot be theSameInstanceAs scopedClient
    YtClientProvider.ytRpcClient(conf) shouldNot be theSameInstanceAs scopedClient
  }

  it should "close only the clients of the closed scope" in withScopes("scope-a", "scope-b") {
    val closedClient = YtClientProvider.scopedYtRpcClient(conf, "scope-a")
    val otherScopeClient = YtClientProvider.scopedYtRpcClient(conf, "scope-b")
    val unscopedClient = YtClientProvider.ytRpcClient(conf)

    YtClientProvider.closeScope("scope-a")

    closedClient.connector.eventLoopGroup().isShuttingDown shouldBe true
    otherScopeClient.connector.eventLoopGroup().isShuttingDown shouldBe false
    unscopedClient.connector.eventLoopGroup().isShuttingDown shouldBe false
    YtClientProvider.scopedYtRpcClient(conf, "scope-a") shouldNot be theSameInstanceAs closedClient
  }

  it should "close every client of the scope even if some of them fail to close" in {
    val failingClients = Seq("proxy-a", "proxy-b").map { proxy =>
      val yt = Mockito.mock(classOf[CompoundClient])
      Mockito.doThrow(new IllegalStateException(s"Failed to close the client for $proxy")).when(yt).close()
      proxy -> YtRpcClient(proxy, yt, Mockito.mock(classOf[DefaultBusConnector]))
    }
    YtClientProvider.getScopedClients("scope-c") = TrieMap(failingClients: _*)

    noException should be thrownBy YtClientProvider.closeScope("scope-c")

    failingClients.foreach { case (_, client) => Mockito.verify(client.yt).close() }
    YtClientProvider.getScopedClients.contains("scope-c") shouldBe false
  }
}
