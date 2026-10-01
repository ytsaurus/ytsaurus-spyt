package tech.ytsaurus.spyt.wrapper.client

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

trait YtClientProvider {
  def ytClient(conf: YtClientConfiguration): CompoundClient = ytClient(conf, None)
  def ytClient(conf: YtClientConfiguration, rpcClientListener: Option[SpytRpcClientListener]): CompoundClient
}

object YtClientProvider extends YtClientProvider {
  private val CLIENT_THREADS_PER_SPARK_CORE: Int = 2
  private val log = LoggerFactory.getLogger(getClass)
  private val clients = TrieMap.empty[String, YtRpcClient] // normalizedProxy - YtRpcClient
  private val scopedClients = TrieMap.empty[String, TrieMap[String, YtRpcClient]] // scope - clients of the scope

  // testing
  private[spyt] def getClients: TrieMap[String, YtRpcClient] = clients
  private[spyt] def getScopedClients: TrieMap[String, TrieMap[String, YtRpcClient]] = scopedClients

  def ytClient(conf: YtClientConfiguration, rpcClientListener: Option[SpytRpcClientListener]): CompoundClient = {
    ytRpcClient(conf, rpcClientListener).yt
  }

  def ytClientWithProxy(conf: YtClientConfiguration, proxy: Option[String],
    rpcClientListener: Option[SpytRpcClientListener] = None): CompoundClient = {
    ytRpcClient(conf.replaceProxy(proxy), rpcClientListener).yt
  }

  def ytRpcClient(conf: YtClientConfiguration,
    rpcClientListener: Option[SpytRpcClientListener] = None): YtRpcClient = this.synchronized {
    cachedRpcClient(clients, conf, rpcClientListener)
  }

  // A scoped client is shared only within its scope, and closeScope closes all clients of the scope.
  def scopedYtRpcClient(conf: YtClientConfiguration, scope: String): YtRpcClient = this.synchronized {
    cachedRpcClient(scopedClients.getOrElseUpdate(scope, TrieMap.empty), conf, None)
  }

  private def cachedRpcClient(
    cache: TrieMap[String, YtRpcClient],
    conf: YtClientConfiguration,
    rpcClientListener: Option[SpytRpcClientListener]): YtRpcClient = {
    val normalizedProxy = conf.normalizedProxy
    val key = cacheKey(normalizedProxy, conf.fixedProxyAddress, rpcClientListener)
    cache.getOrElseUpdate(key, {
      val clientThreads = getClientThreads
      log.info(s"Create YtClient for proxy $normalizedProxy and $clientThreads clientThreads")
      YtWrapper.createRpcClient(conf, clientThreads, rpcClientListener)
    })
  }

  private def cacheKey(normalizedProxy: String, fixedProxyAddress: Option[String],
    rpcClientListener: Option[SpytRpcClientListener]): String =
    Seq(normalizedProxy, fixedProxyAddress.getOrElse(""), rpcClientListener.map(_.id).getOrElse("")).mkString(";")

  // A client that fails to close must not keep the other clients of the scope open or hide an error of the caller.
  def closeScope(scope: String): Unit = this.synchronized {
    log.info(s"Close YT Clients of scope $scope")
    scopedClients.remove(scope).foreach(_.values.foreach { client =>
      try {
        client.close()
      } catch {
        case NonFatal(e) =>
          log.warn(s"Failed to close YT Client for proxy ${client.normalizedProxy} of scope $scope", e)
      }
    })
  }

  def close(): Unit = this.synchronized {
    log.info(s"Close all YT Clients")
    clients.foreach(_._2.close())
    clients.clear()
    scopedClients.keys.foreach(closeScope)
  }

  def close(id: String): Unit = this.synchronized {
    log.info(s"Close YT Client for id $id")
    clients.get(id).foreach(_.close())
    clients.remove(id)
  }

  private def getClientThreads: Int = {
    val confOpt = Option(SparkEnv.get) match {
      case Some(env) => Some(env.conf)
      case None => SparkSession.getDefaultSession.map(_.sparkContext.getConf)
    }
    val cores = confOpt match {
      case Some(conf) => if (SparkSession.getDefaultSession.nonEmpty) {
        conf.getInt("spark.driver.cores", 1)
      } else {
        conf.getOption("spark.executor.cores").map(_.toInt).getOrElse(1)
      }
      case None => 1
    }

    cores * CLIENT_THREADS_PER_SPARK_CORE
  }
}
