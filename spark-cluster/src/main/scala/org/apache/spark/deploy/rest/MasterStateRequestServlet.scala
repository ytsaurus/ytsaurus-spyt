package org.apache.spark.deploy.rest

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.deploy.DeployMessages
import org.apache.spark.deploy.master.WorkerInfo
import org.apache.spark.rpc.RpcEndpointRef

/**
 * A servlet for handling master state requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class MasterStateRequestServlet extends RestServlet with RestServletCompat {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  override def processGet(
    pathInfo: String,
    getParameter: String => String): (SubmitRestProtocolResponse, Option[Int]) = {
    try {
      handleMasterState() -> None
    } catch {
      // The client failed to provide a valid JSON, so this is not our fault
      case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
        handleError("Malformed request: " + formatException(e)) -> Some(400)
    }
  }

  protected def handleMasterState(): MasterStateResponse
}

/**
 * A response to a status request in the REST application submission protocol.
 */
private[spark] class MasterStateResponse extends SubmitRestProtocolResponse {
  var workers: Array[WorkerInfo] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(workers, "workers")
  }
}

private[rest] class StandaloneMasterStateRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends MasterStateRequestServlet {

  override protected def handleMasterState(): MasterStateResponse = {
    val response = masterEndpoint.askSync[DeployMessages.MasterStateResponse](
      DeployMessages.RequestMasterState)
    val masterStateResponse = new MasterStateResponse
    masterStateResponse.workers = response.workers
    masterStateResponse.serverSparkVersion = sparkVersion
    masterStateResponse
  }
}