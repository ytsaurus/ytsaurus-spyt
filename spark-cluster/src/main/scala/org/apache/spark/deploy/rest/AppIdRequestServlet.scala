package org.apache.spark.deploy.rest

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.launcher.DeployMessages


/**
 * A servlet for handling get application id requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class AppIdRequestServlet extends RestServlet with RestServletCompat {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  override def processGet(
    pathInfo: String,
    getParameter: String => String): (SubmitRestProtocolResponse, Option[Int]) = {
    parseSubmissionId(pathInfo).map { submissionId =>
      handleGetAppId(submissionId) -> None
    }.getOrElse {
      handleError("Submission ID is missing in status request.") -> Some(400)
    }
  }

  protected def handleGetAppId(submissionId: String): AppIdRestResponse
}


/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class AppIdRestResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var appId: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}


private[rest] class StandaloneAppIdRequestServlet(
                                                   masterEndpoint: RpcEndpointRef,
                                                   conf: SparkConf)
  extends AppIdRequestServlet {
  override protected def handleGetAppId(submissionId: String): AppIdRestResponse = {
    val response = masterEndpoint.askSync[DeployMessages.AppIdResponse](
      DeployMessages.RequestAppId(submissionId))
    val appIdResponse = new AppIdRestResponse
    appIdResponse.submissionId = submissionId
    appIdResponse.success = true
    appIdResponse.appId = response.appId.orNull
    appIdResponse.serverSparkVersion = sparkVersion
    appIdResponse
  }
}
