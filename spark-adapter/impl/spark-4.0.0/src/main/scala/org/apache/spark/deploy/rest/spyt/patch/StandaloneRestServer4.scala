package org.apache.spark.deploy.rest.spyt.patch

import org.apache.spark.SparkConf
import org.apache.spark.deploy.rest.RestSubmitSupport.{instance => rss}
import org.apache.spark.deploy.rest.{ErrorServlet, RestServlet, RestSubmissionServer, StandaloneClearRequestServlet, StandaloneKillAllRequestServlet, StandaloneKillRequestServlet, StandaloneReadyzRequestServlet, StandaloneSubmitRequestServlet, StatusRequestServlet}
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.patch.annotations.{Applicability, OriginClass}

@OriginClass("org.apache.spark.deploy.rest.StandaloneRestServer")
@Applicability(from = "4.0.0")
private[deploy] class StandaloneRestServer4(
  host: String,
  requestedPort: Int,
  masterConf: SparkConf,
  masterEndpoint: RpcEndpointRef,
  masterUrl: String) extends RestSubmissionServer(null, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  protected override val killAllRequestServlet =
    new StandaloneKillAllRequestServlet(masterEndpoint, masterConf)
  protected override val statusRequestServlet: StatusRequestServlet = null
  protected override val clearRequestServlet =
    new StandaloneClearRequestServlet(masterEndpoint, masterConf)
  protected override val readyzRequestServlet =
    new StandaloneReadyzRequestServlet(masterEndpoint, masterConf)

  private val ytStatusRequestServlet = rss.statusRequestServlet(masterEndpoint, masterConf)
  private val masterStateRequestServlet = rss.masterStateRequestServlet(masterEndpoint, masterConf)
  private val appIdRequestServlet = rss.appIdRequestServlet(masterEndpoint, masterConf)
  private val appStatusRequestServlet = rss.appStatusRequestServlet(masterEndpoint, masterConf)
  private val spytConnectServerServlet = rss.spytConnectServerServlet(masterEndpoint, masterUrl, masterConf)

  protected override lazy val contextToServlet: Map[String, RestServlet] = Map(
    s"$baseContext/create/*" -> submitRequestServlet,
    s"$baseContext/kill/*" -> killRequestServlet,
    s"$baseContext/killall/*" -> killAllRequestServlet,
    s"$baseContext/status/*" -> ytStatusRequestServlet,
    s"$baseContext/clear/*" -> clearRequestServlet,
    s"$baseContext/readyz/*" -> readyzRequestServlet,
    s"$baseContext/master/*" -> masterStateRequestServlet,
    s"$baseContext/getAppId/*" -> appIdRequestServlet,
    s"$baseContext/getAppStatus/*" -> appStatusRequestServlet,
    s"$baseContext/spytConnectServer/*" -> spytConnectServerServlet,
    "/*" -> new ErrorServlet // default handler
  )
}
