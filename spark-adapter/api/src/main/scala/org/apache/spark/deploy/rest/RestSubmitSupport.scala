package org.apache.spark.deploy.rest

import org.apache.spark.SparkConf
import org.apache.spark.rpc.RpcEndpointRef

import java.util.ServiceLoader

trait RestSubmitSupport {
  def createSubmission(appResource: String,
                       mainClass: String,
                       appArgs: Array[String],
                       conf: SparkConf,
                       env: Map[String, String],
                       master: String): SubmitRestProtocolResponse
  def statusRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): StatusRequestServlet
  def masterStateRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet
  def appIdRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet
  def appStatusRequestServlet(masterEndpoint: RpcEndpointRef, masterConf: SparkConf): RestServlet
  def spytConnectServerServlet(masterEndpoint: RpcEndpointRef, masterUrl: String, masterConf: SparkConf): RestServlet
}

object RestSubmitSupport {
  lazy val instance: RestSubmitSupport = ServiceLoader.load(classOf[RestSubmitSupport]).findFirst().get()
}