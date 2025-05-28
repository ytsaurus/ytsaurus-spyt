package tech.ytsaurus.spyt.wrapper.client

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.collection.concurrent.TrieMap

trait YtClientProvider {
  def ytClient(conf: YtClientConfiguration): CompoundClient
}

object YtClientProvider extends YtClientProvider {
  private val CLIENT_THREADS_PER_SPARK_CORE: Int = 2
  private val log = LoggerFactory.getLogger(getClass)
  private val clients = TrieMap.empty[String, YtRpcClient] // normalizedProxy - YtRpcClient

  // testing
  private[spyt] def getClients: TrieMap[String, YtRpcClient] = clients

  def ytClient(conf: YtClientConfiguration): CompoundClient = ytRpcClient(conf).yt

  def ytClientWithProxy(conf: => YtClientConfiguration, proxy: Option[String]): CompoundClient = {
    ytRpcClientWithProxy(conf, proxy).yt
  }

  def ytRpcClientWithProxy(conf: => YtClientConfiguration, proxy: Option[String]): YtRpcClient = {
    ytRpcClient(conf.replaceProxy(proxy))
  }

  def ytRpcClient(conf: => YtClientConfiguration): YtRpcClient = this.synchronized {
    val normalizedProxy = conf.normalizedProxy
    clients.getOrElseUpdate(normalizedProxy, {
      val clientThreads = getClientThreads
      log.info(s"Create YtClient for proxy $normalizedProxy and $clientThreads clientThreads")
      YtWrapper.createRpcClient(conf, clientThreads)
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
