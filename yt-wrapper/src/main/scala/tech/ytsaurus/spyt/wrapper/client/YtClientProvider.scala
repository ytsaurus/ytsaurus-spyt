package tech.ytsaurus.spyt.wrapper.client

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap

object YtClientProvider {
  private val log = LoggerFactory.getLogger(getClass)

  private val clients = TrieMap.empty[String, YtRpcClient]

  private def threadId: String = Thread.currentThread().getId.toString

  def ytClient(conf: => YtClientConfiguration, id: String): CompoundClient = ytRpcClient(conf, id).yt

  def ytClientWithProxy(conf: => YtClientConfiguration, proxy: Option[String], id: String = threadId): CompoundClient = {
    ytRpcClientWithProxy(conf, proxy, id).yt
  }

  // testing
  private[spyt] def getClients: TrieMap[String, YtRpcClient] = clients

  // for java
  def ytClient(conf: YtClientConfiguration): CompoundClient = ytRpcClient(conf, threadId).yt

  def cachedClient(id: String): YtRpcClient = this.synchronized { clients(id) }

  private def genId(proxy: Option[String] = None, id: String = threadId): String = s"$id-${proxy.orNull}"

  def ytRpcClientWithProxy(conf: => YtClientConfiguration, proxy: Option[String], id: String = threadId): YtRpcClient = {
    ytRpcClient(conf.replaceProxy(proxy), genId(proxy, id))
  }

  def ytRpcClient(conf: => YtClientConfiguration, id: String = threadId): YtRpcClient = this.synchronized {
    clients.getOrElseUpdate(id, {
      log.info(s"Create YtClient for id $id")
      YtWrapper.createRpcClient(id, conf)
    })
  }

  def close(): Unit = this.synchronized {
    log.info(s"Close all YT Clients")
    clients.foreach(_._2.close())
    clients.clear()
  }

  def close(id: String): Unit = this.synchronized {
    log.info(s"Close YT Client for id $id")
    clients.get(id).foreach(_.close())
    clients.remove(id)
  }

  def closeByPrefix(id: String): Unit = this.synchronized {
    log.info(s"Close YT Clients for prefix $id")
    clients.keys.foreach { key =>
      if (key.startsWith(id)) clients.remove(key).foreach(_.close())
    }
  }
}
