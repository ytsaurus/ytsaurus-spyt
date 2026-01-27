package org.apache.spark.deploy.rest

import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, EXECUTOR_MEMORY}
import org.apache.spark.{SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.connect.ytsaurus.SpytConnectServer.INNER_CLUSTER
import org.apache.spark.sql.connect.ytsaurus.Config.YTSAURUS_CONNECT_STARTUP_TIMEOUT
import tech.ytsaurus.spyt.launcher.DeployMessages

import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.io.Source

class StartConnectServerServlet(masterEndpoint: RpcEndpointRef, masterUrl: String, conf: SparkConf)
  extends StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, conf) {

  override def doPost(requestServlet: HttpServletRequest, responseServlet: HttpServletResponse): Unit = {
    val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
    val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
    requestMessage.validate()
    val response = requestMessage match {
      case request: StartConnectServerRequest =>
        val (requestToken, endpointFuture) = registerEndpointRequest()
        startSpytConnectServer(requestToken, request)
        waitForResponse(endpointFuture, requestToken, responseServlet)
      case _ =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError("Invalid request for start connect server endpoint")
    }
    sendResponse(response, responseServlet)
  }

  private def registerEndpointRequest(): (String, Future[String]) = {
    val requestToken = UUID.randomUUID().toString
    val endpointFuture = masterEndpoint.askSync[DeployMessages.WaitSpytConnectEndpointResponse](
      DeployMessages.WaitSpytConnectEndpointRequest(requestToken)).endpointFuture
    requestToken -> endpointFuture
  }

  private def startSpytConnectServer(requestToken: String, request: StartConnectServerRequest): Unit = {
    val optUser = request.sparkConf.get("spark.hadoop.yt.user")
      .map(user => Map("user.name" -> user))
      .getOrElse(Map.empty)
    val submitConf = Map(conf.getAll ++ request.sparkConf: _*) ++ Map(
      DRIVER_MEMORY.key -> request.driverMemory,
      EXECUTOR_MEMORY.key -> request.executorMemory,
      EXECUTOR_INSTANCES.key -> request.numExecutors.toString,
      EXECUTOR_CORES.key -> request.executorCores.toString,
      "spark.connect.grpc.binding.port" -> request.grpcPortStart.toString
    ) ++ optUser

    val submitRequest = new CreateSubmissionRequest()
    submitRequest.appResource = ""
    submitRequest.mainClass = "org.apache.spark.sql.connect.ytsaurus.SpytConnectServer"
    submitRequest.appArgs = Array(INNER_CLUSTER, masterEndpoint.address.toSparkURL, requestToken)
    submitRequest.sparkProperties = submitConf
    submitRequest.environmentVariables = Map.empty
    submitRequest.clientSparkVersion = sparkVersion

    handleSubmit(submitRequest.toJson, submitRequest, null)
  }

  private def waitForResponse(
    endpointFuture: Future[String],
    requestToken: String,
    responseServlet: HttpServletResponse
  ): SubmitRestProtocolResponse = {
    val timeout = Duration(conf.get(YTSAURUS_CONNECT_STARTUP_TIMEOUT), TimeUnit.MILLISECONDS)
    try {
      val endpoint = Await.result(endpointFuture, timeout)

      val responseMsg = new StartConnectServerResponse()
      responseMsg.success = true
      responseMsg.serverSparkVersion = sparkVersion
      responseMsg.endpoint = endpoint
      responseMsg
    } catch {
      case _: TimeoutException =>
        responseServlet.setStatus(HttpServletResponse.SC_GATEWAY_TIMEOUT)
        handleError(s"Didn't get the connect gateway address within ${timeout.toSeconds} seconds")
    } finally {
      masterEndpoint.send(DeployMessages.RemoveWaitSpytConnectEndpointToken(requestToken))
    }
  }
}

private[rest] class StartConnectServerRequest extends SubmitRestProtocolMessage {
  var driverMemory: String = null
  var numExecutors: Int = -1
  var executorCores: Int = -1
  var executorMemory: String = null
  var grpcPortStart: Int = -1
  var sparkConf: Map[String, String] = Map()

  override protected def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(driverMemory, "driverMemory")
    assertFieldIsSet(executorMemory, "executorMemory")
    assert(numExecutors > 0, "numExecutors must be set and be positive")
    assert(executorCores > 0, "executorCores must be set and be positive")
    assert(grpcPortStart > 0, "grpcPortStart must be set and be positive")
  }
}

private[spark] class StartConnectServerResponse extends SubmitRestProtocolResponse {
  var endpoint: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(endpoint, "endpoint")
  }
}
