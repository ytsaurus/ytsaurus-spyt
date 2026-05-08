package org.apache.spark.deploy.rest

import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, EXECUTOR_MEMORY}
import org.apache.spark.{SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.connect.ytsaurus.SpytConnectServer.INNER_CLUSTER
import org.apache.spark.sql.connect.ytsaurus.Config.{YTSAURUS_CONNECT_SETTINGS_HASH, YTSAURUS_CONNECT_STARTUP_TIMEOUT}
import tech.ytsaurus.spyt.launcher.DeployMessages
import tech.ytsaurus.spyt.launcher.DeployMessages.SpytConnectApplication

import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.io.Source

class SpytConnectServerServlet(masterEndpoint: RpcEndpointRef, masterUrl: String, conf: SparkConf)
  extends StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, conf) {

  override def doGet(requestServlet: HttpServletRequest, responseServlet: HttpServletResponse): Unit = {
    val user = requestServlet.getParameter("user")

    val response = handleGetExistingConnectApps(user, responseServlet)
    sendResponse(response, responseServlet)
  }

  private def handleGetExistingConnectApps(
    user: String,
    responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    if (user == null) {
      responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      return handleError("user must be set in query parameters")
    }

    val appsResponse = masterEndpoint.askSync[DeployMessages.FindSpytConnectAppsResponse](
        DeployMessages.FindSpytConnectAppsRequest(user))

    val response = new SpytConnectServerResponses()
    response.apps = appsResponse.apps.map(createSpytConnectServerResponse)
    setCommonResponseFields(response)
    response
  }

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

  private def registerEndpointRequest(): (String, Future[SpytConnectApplication]) = {
    val requestToken = UUID.randomUUID().toString
    val connectAppFuture = masterEndpoint.askSync[DeployMessages.WaitSpytConnectEndpointResponse](
      DeployMessages.WaitSpytConnectEndpointRequest(requestToken)).connectAppFuture
    requestToken -> connectAppFuture
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
    endpointFuture: Future[SpytConnectApplication],
    requestToken: String,
    responseServlet: HttpServletResponse
  ): SubmitRestProtocolResponse = {
    val timeout = Duration(conf.get(YTSAURUS_CONNECT_STARTUP_TIMEOUT), TimeUnit.MILLISECONDS)
    try {
      val endpoint = Await.result(endpointFuture, timeout)
      createSpytConnectServerResponse(endpoint)
    } catch {
      case _: TimeoutException =>
        responseServlet.setStatus(HttpServletResponse.SC_GATEWAY_TIMEOUT)
        handleError(s"Didn't get the connect gateway address within ${timeout.toSeconds} seconds")
    } finally {
      masterEndpoint.send(DeployMessages.RemoveWaitSpytConnectEndpointToken(requestToken))
    }
  }

  private def createSpytConnectServerResponse(application: SpytConnectApplication): SpytConnectServerResponse = {
    import application._
    val responseMsg = new SpytConnectServerResponse()
    responseMsg.endpoint = endpoint
    responseMsg.driverId = driverId
    settingsHashOpt.foreach(settingsHash => responseMsg.settingsHash = settingsHash)
    setCommonResponseFields(responseMsg)
    responseMsg
  }

  private def setCommonResponseFields(responseMsg: SubmitRestProtocolResponse): Unit = {
    responseMsg.serverSparkVersion = sparkVersion
    responseMsg.success = true
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

private[spark] class SpytConnectServerResponse extends SubmitRestProtocolResponse {
  var endpoint: String = null
  var driverId: String = null
  var settingsHash: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(endpoint, "endpoint")
    assertFieldIsSet(driverId, "driverId")
  }
}

private[spark] class SpytConnectServerResponses extends SubmitRestProtocolResponse {
  var apps: Seq[SpytConnectServerResponse] = null

  override protected def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(apps, "apps")
    apps.foreach { app =>
      app.validate()
    }
  }
}
