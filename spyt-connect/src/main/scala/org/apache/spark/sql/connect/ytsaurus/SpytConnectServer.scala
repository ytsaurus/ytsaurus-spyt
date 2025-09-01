package org.apache.spark.sql.connect.ytsaurus

import org.apache.spark.SparkEnv
import org.apache.spark.deploy.ytsaurus.YTsaurusUtils
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_BINDING_PORT
import org.apache.spark.sql.connect.service.{SparkConnectServer, SparkConnectService}
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.YTsaurusClient
import tech.ytsaurus.client.request.UpdateOperationParameters
import tech.ytsaurus.client.rpc.YTsaurusClientAuth
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.Utils
import tech.ytsaurus.ysontree.YTree

import java.net.ServerSocket
import scala.annotation.tailrec
import scala.util.control.NonFatal

object SpytConnectServer {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    checkAndUpdateGrpcPort()
    val serverRunner: Runnable = () => {
      SparkConnectServer.main(args)
    }
    val serverThread = new Thread(serverRunner, "Spark connect server")
    serverThread.start()

    waitForGrpcServerStart()
    addGrpcEndpointToAnnotation()

    val idleTimeout = SparkEnv.get.conf.get(Config.YTSAURUS_CONNECT_IDLE_TIMEOUT)
    while(keepListening(idleTimeout)) {
      Thread.sleep(10000)
    }

    log.info(s"Idle timeout of ${idleTimeout}ms has passed, shutting down SPYT connect server")
    SparkConnectService.stop()

    serverThread.join()
  }

  private val MAX_PORT_RETRIES = 32

  private def checkAndUpdateGrpcPort(): Unit = {
    val startingPort = sys.props(CONNECT_GRPC_BINDING_PORT.key).toInt
    log.info(s"Checking that grpc port $startingPort is available")
    for (port <- startingPort until (startingPort + MAX_PORT_RETRIES)) {
      if (isPortFree(port)) {
        if (port != startingPort) {
          log.info(s"Found $port free port instead of $startingPort")
          sys.props(CONNECT_GRPC_BINDING_PORT.key) = port.toString
        }
        return
      }
      log.info(s"Port $port is busy, checking the next port")
    }
    log.error("Couldn't find free port for Spark Connect gRPC service, aborting")
    System.exit(-1)
  }

  private def isPortFree(port: Int): Boolean = {
    var ss: ServerSocket = null
    try {
      ss = new ServerSocket(port)
      true
    } catch {
      case NonFatal(_) => false
    } finally {
      if (ss != null) try ss.close() catch { case _: Throwable => () }
    }
  }

  @tailrec
  private def waitForGrpcServerStart(): Unit = {
    if (SparkConnectService.listener == null) {
      Thread.sleep(1000)
      waitForGrpcServerStart()
    }
  }

  private def keepListening(idleTimeout: Long): Boolean = SparkConnectService.listActiveExecutions match {
    case Left(lastExecutionFinishTime) => System.currentTimeMillis() - lastExecutionFinishTime <= idleTimeout
    case _ => true
  }

  private def addGrpcEndpointToAnnotation(): Unit = {
    val operationId = System.getenv("YT_OPERATION_ID")
    if (operationId != null) {
      val host = Utils.ytHostnameOrIpAddress
      val port = SparkConnectService.localPort
      val endpoint = s"$host:$port"

      val conf = SparkEnv.get.conf
      val ytProxy = YTsaurusUtils.parseMasterUrl(conf.get("spark.master"))
      val token = YTsaurusUtils.getToken(conf)
      val clientBuilder: YTsaurusClient.ClientBuilder[_ <: YTsaurusClient, _] = YTsaurusClient.builder()
      clientBuilder.setCluster(ytProxy)
      clientBuilder.setAuth(YTsaurusClientAuth.builder().setToken(token).build())
      val client: YTsaurusClient = clientBuilder.build()
      try {
        val annotations = YTree.mapBuilder().key("spark_connect_endpoint").value(endpoint).buildMap()
        val req = UpdateOperationParameters
          .builder()
          .setOperationId(GUID.valueOf(operationId))
          .setAnnotations(annotations)
          .build()

        client.updateOperationParameters(req).join()
      } finally {
        client.close()
      }
    }
  }
}
