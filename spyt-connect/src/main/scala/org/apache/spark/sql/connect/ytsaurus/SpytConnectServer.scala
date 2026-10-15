package org.apache.spark.sql.connect.ytsaurus

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.connect.config.Connect.{CONNECT_GRPC_BINDING_PORT, CONNECT_GRPC_INTERCEPTOR_CLASSES}
import org.apache.spark.sql.connect.service.{SparkConnectServer, SparkConnectService}
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.request.UpdateOperationParameters
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.SparkVersionUtils
import tech.ytsaurus.spyt.wrapper.Utils
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.ysontree.YTree

import java.net.ServerSocket
import scala.annotation.tailrec
import scala.util.control.NonFatal

object SpytConnectServer {
  private val log = LoggerFactory.getLogger(getClass)
  val INNER_CLUSTER: String = "inner-cluster"

  private case class InnerClusterParameters(masterEndpoint: String, requestToken: String)

  def main(args: Array[String]): Unit = {

    val innerClusterOpt: Option[InnerClusterParameters] = args match {
      case Array(INNER_CLUSTER, masterEndpoint: String, requestToken: String) =>
        checkUserAndTokenExistence()
        Some(InnerClusterParameters(masterEndpoint, requestToken))
      case _ => None
    }

    checkAndUpdateGrpcPort()
    val forwardingEndpoint = sys.props.get(Config.YTSAURUS_CONNECT_FORWARD_ENDPOINT.key)
    SpytConnectForwardingInterceptor.validateConfig(
      sys.props.get(Config.YTSAURUS_CONNECT_FORWARD_METHODS.key),
      sys.props.get(Config.YTSAURUS_CONNECT_FORWARD_MIRROR_METHODS.key),
      forwardingEndpoint)
    if (forwardingEndpoint.exists(_.nonEmpty)) {
      registerForwardingInterceptor()
    }
    val serverRunner: Runnable = () => {
      var exitCode = 1
      try {
        SparkConnectServer.main(args)
        exitCode = 0
      } catch {
        case error: Throwable => log.error("Spark connect server failed", error)
      } finally {
        System.exit(exitCode)
      }
    }
    val serverThread = new Thread(serverRunner, "Spark connect server")
    serverThread.start()

    waitForGrpcServerStart()

    val sparkConf = SparkEnv.get.conf

    innerClusterOpt match {
      case Some(innerClusterParams) => sendGrpcEndpointToMaster(
        innerClusterParams,
        sparkConf.get("spark.hadoop.yt.user"),
        sparkConf.get("spark.driverId"),
        sparkConf.getAppId,
        sparkConf.get(Config.YTSAURUS_CONNECT_SETTINGS_HASH)
      )
      case None => addGrpcEndpointToAnnotation(sparkConf)
    }

    val idleTimeout = sparkConf.get(Config.YTSAURUS_CONNECT_IDLE_TIMEOUT)

    while (keepListening(idleTimeout) && serverThread.isAlive) {
      Thread.sleep(10000)
    }

    if (serverThread.isAlive) {
      log.info(s"Idle timeout of ${idleTimeout}ms has passed, shutting down SPYT connect server")
      SparkConnectService.stop()
    }

    serverThread.join()
  }

  private def checkUserAndTokenExistence(): Unit = {
    List("user", "token").foreach { field =>
      val key = s"spark.hadoop.yt.$field"
      if (sys.props.get(key).isEmpty) {
        log.error(s"$key property must be specified when submitting to an inner cluster")
        System.exit(-1)
      }
    }
  }

  private val MAX_PORT_RETRIES = 32

  private def registerForwardingInterceptor(): Unit = {
    val key = CONNECT_GRPC_INTERCEPTOR_CLASSES.key
    val interceptor = classOf[SpytConnectForwardingInterceptor].getName
    val configured = sys.props.get(key).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
    if (!configured.contains(interceptor)) {
      sys.props(key) = (configured :+ interceptor).mkString(",")
    }
  }

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
    case Left(lastExecutionFinishTime) =>
      val (currentTime, multiplier) = if (SparkVersionUtils.lessThan("4.0.0")) {
        (System.currentTimeMillis(), 1L)
      } else {
        (System.nanoTime(), 1000000L)
      }
      currentTime - lastExecutionFinishTime <= idleTimeout * multiplier
    case _ => true
  }

  private def getEndpoint: String = {
    val host = Utils.ytHostnameOrIpAddress
    val port = SparkConnectService.localPort
    s"$host:$port"
  }

  private def addGrpcEndpointToAnnotation(sparkConf: SparkConf): Unit = {
    val operationId = System.getenv("YT_OPERATION_ID")
    if (operationId != null) {
      val client = YtClientProvider.ytClient(ytClientConfiguration(sparkConf))
      val annotations = YTree.mapBuilder().key("spark_connect_endpoint").value(getEndpoint).buildMap()
      val req = UpdateOperationParameters
        .builder()
        .setOperationId(GUID.valueOf(operationId))
        .setAnnotations(annotations)
        .build()

      client.updateOperationParameters(req).join()
    }
  }

  private def sendGrpcEndpointToMaster(
    innerClusterParams: InnerClusterParameters,
    user: String,
    driverId: String,
    applicationId: String,
    settingsHashOpt: Option[String]): Unit = {
    val masterRpcAddress = RpcAddress.fromSparkURL(innerClusterParams.masterEndpoint)
    val masterRef = SparkEnv.get.rpcEnv.setupEndpointRef(masterRpcAddress, "Master")
    masterRef.send(SpytConnectAppStartedMessage(
      innerClusterParams.requestToken,
      user,
      driverId,
      applicationId,
      getEndpoint,
      settingsHashOpt
    ))
  }
}

case class SpytConnectAppStartedMessage(
  requestToken: String,
  user: String,
  driverId: String,
  applicationId: String,
  endpoint: String,
  settingsHashOpt: Option[String]
)
