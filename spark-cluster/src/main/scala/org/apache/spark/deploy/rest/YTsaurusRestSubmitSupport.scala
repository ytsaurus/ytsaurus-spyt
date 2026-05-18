package org.apache.spark.deploy.rest

import org.apache.spark.SparkConf
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.SparkAdapter

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

  override def statusRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    SparkAdapter.instance.wrapRestServlet(new YtStatusRequestServlet(masterEndpoint, masterConf))
      .asInstanceOf[RestServlet]
  }

  override def masterStateRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    SparkAdapter.instance.wrapRestServlet(new StandaloneMasterStateRequestServlet(masterEndpoint, masterConf))
      .asInstanceOf[RestServlet]
  }

  override def appIdRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    SparkAdapter.instance.wrapRestServlet(new StandaloneAppIdRequestServlet(masterEndpoint, masterConf))
      .asInstanceOf[RestServlet]
  }

  override def appStatusRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet = {
    SparkAdapter.instance.wrapRestServlet(new StandaloneAppStatusRequestServlet(masterEndpoint, masterConf))
      .asInstanceOf[RestServlet]
  }

  override def spytConnectServerServlet(
    masterEndpoint: RpcEndpointRef,
    masterUrl: String,
    masterConf: SparkConf): RestServlet = {
    SparkAdapter.instance.wrapRestServlet(new SpytConnectServerServlet(masterEndpoint, masterUrl, masterConf))
      .asInstanceOf[RestServlet]
  }
}
