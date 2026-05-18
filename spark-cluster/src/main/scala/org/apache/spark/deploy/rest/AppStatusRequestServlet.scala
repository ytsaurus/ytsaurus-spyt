package org.apache.spark.deploy.rest

import org.apache.spark.{SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.launcher.DeployMessages

import java.util.Date

/**
 * A servlet for handling get application id requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class AppStatusRequestServlet extends RestServlet with RestServletCompat {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  override def processGet(
    pathInfo: String,
    getParameter: String => String): (SubmitRestProtocolResponse, Option[Int]) = {
    parseSubmissionId(pathInfo) match {
      case Some(appId) =>
        handleGetAppStatus(appId) -> None
      case None =>
        handleGetAllAppStatuses() -> None
    }
  }

  protected def handleGetAppStatus(appId: String): AppStatusRestResponse

  protected def handleGetAllAppStatuses(): AppStatusesRestResponse

}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class AppStatusRestResponse extends SubmitRestProtocolResponse {
  var appId: String = null
  var appState: String = null
  var appSubmittedAt: Date = null
  var appStartedAt: Long = -1L

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(appId, "appId")
    assertFieldIsSet(appState, "appState")
    assertFieldIsSet(success, "success")
    assertFieldIsSet(appStartedAt, "appStartedAt")
  }
}

private[spark] class AppStatusesRestResponse extends SubmitRestProtocolResponse {
  var statuses: Seq[AppStatusRestResponse] = Seq()

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(statuses, "statuses")
  }
}

private[rest] class StandaloneAppStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends AppStatusRequestServlet {

  override protected def handleGetAppStatus(appId: String): AppStatusRestResponse = {
    val response = masterEndpoint.askSync[DeployMessages.ApplicationStatusResponse](
      DeployMessages.RequestApplicationStatus(appId))
    val appStatusResponse = new AppStatusRestResponse
    appStatusResponse.appId = appId
    appStatusResponse.success = response.found
    appStatusResponse.appState = response.info.map(_.state).getOrElse("UNDEFINED")
    appStatusResponse.appSubmittedAt = response.info.map(_.submitDate).orNull
    appStatusResponse.appStartedAt = response.info.map(_.startTime).getOrElse(-1L)
    appStatusResponse.serverSparkVersion = sparkVersion
    appStatusResponse
  }

  override protected def handleGetAllAppStatuses(): AppStatusesRestResponse = {
    val response = masterEndpoint.askSync[DeployMessages.ApplicationStatusesResponse](
      DeployMessages.RequestApplicationStatuses)
    val statusesRestResponse = new AppStatusesRestResponse
    statusesRestResponse.statuses = response.statuses.map { info =>
      val status = new AppStatusRestResponse
      status.appId = info.id
      status.appState = info.state
      status.appSubmittedAt = info.submitDate
      status.appStartedAt = info.startTime
      status.serverSparkVersion = sparkVersion
      status.success = true
      status
    }
    statusesRestResponse.success = true
    statusesRestResponse.serverSparkVersion = sparkVersion
    statusesRestResponse
  }
}
