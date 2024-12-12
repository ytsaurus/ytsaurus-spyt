package org.apache.spark.deploy.rest

import org.apache.spark.SparkConf
import org.apache.spark.rpc.RpcEndpointRef

class YTsaurusRestSubmitSupport extends RestSubmitSupport {

  override def createSubmission(appResource: String,
                                mainClass: String,
                                appArgs: Array[String],
                                conf: SparkConf,
                                env: Map[String, String],
                                master: String): SubmitRestProtocolResponse = {
    val sparkProperties = conf.getAll.toMap
    val client = new RestSubmissionClientSpyt(master)
    val submitRequest = client.constructSubmitRequest(
      appResource, mainClass, appArgs, sparkProperties, env)
    client.createSubmission(submitRequest)
  }

  override def statusRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): StatusRequestServlet = {
    new YtStatusRequestServlet(masterEndpoint, masterConf)
  }

  override def masterStateRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    new StandaloneMasterStateRequestServlet(masterEndpoint, masterConf)
  }

  override def appIdRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    new StandaloneAppIdRequestServlet(masterEndpoint, masterConf)
  }

  override def appStatusRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    new StandaloneAppStatusRequestServlet(masterEndpoint, masterConf)
  }
}
