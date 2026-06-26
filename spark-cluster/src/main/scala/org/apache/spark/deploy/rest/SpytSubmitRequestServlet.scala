package org.apache.spark.deploy.rest

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.spark.SparkConf
import org.apache.spark.deploy.{DeployMessages, SparkSubmit, SparkSubmitArguments}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils
import tech.ytsaurus.spyt.SparkVersionUtils
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.config.Utils.{parseRemoteConfig, remoteClusterConfigPath, remoteGlobalConfigPath, remoteVersionConfigPath}
import tech.ytsaurus.spyt.wrapper.discovery.CypressDiscoveryService

import java.io.InputStream
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

class SpytSubmitRequest extends CreateSubmissionRequest

private class UpstreamSubmitServlet(masterEndpoint: RpcEndpointRef, masterUrl: String, masterConf: SparkConf)
  extends StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf) {
  def submitDirectly(json: String, msg: SubmitRestProtocolMessage): SubmitRestProtocolResponse =
    handleSubmit(json, msg, null)
}

class SpytSubmitRequestServlet(masterEndpoint: RpcEndpointRef, masterUrl: String, masterConf: SparkConf)
  extends RestServlet with RestServletCompat with Logging {

  import SpytSubmitRequestServlet._

  private val upstreamServlet = new UpstreamSubmitServlet(masterEndpoint, masterUrl, masterConf)

  override def processPost(getInputStream: () => InputStream): (SubmitRestProtocolResponse, Option[Int]) = {
    val src = Source.fromInputStream(getInputStream())
    val json = try {
      src.mkString
    } catch {
      case NonFatal(e) =>
        val prefix = "Failed to read request body"
        logError(prefix, e)
        return handleError(s"$prefix: ${Utils.exceptionString(e)}") -> Some(400)
    } finally {
      src.close()
    }
    try {
      SubmitRestProtocolMessage.fromJson(json) match {
        case req: SpytSubmitRequest =>
          req.validate()
          val upstream = buildUpstreamRequest(req)
          upstreamServlet.submitDirectly(upstream.toJson, upstream) -> None
        case _ =>
          handleError("Invalid request for spytSubmit endpoint") -> Some(400)
      }
    } catch {
      case NonFatal(e) =>
        val (prefix, code) = e match {
          case _: JsonProcessingException | _: SubmitRestProtocolException | _: IllegalArgumentException =>
            ("Malformed request", 400)
          case _: org.apache.spark.SparkException =>
            ("Failed to prepare submit environment", 400)
          case _ =>
            ("Unhandled error in spytSubmit", 500)
        }
        logError(prefix, e)
        handleError(s"$prefix: ${Utils.exceptionString(e)}") -> Some(code)
    }
  }

  private[rest] def buildUpstreamRequest(req: SpytSubmitRequest): CreateSubmissionRequest = {
    val classArgs = Option(req.mainClass).map(c => Seq("--class", c)).getOrElse(Seq.empty)
    val allProps = Option(req.sparkProperties).getOrElse(Map.empty[String, String]) ++
      Map("spark.master" -> masterUrl, "spark.submit.deployMode" -> "cluster")
    val confArgs = allProps.flatMap { case (k, v) => Seq("--conf", s"$k=$v") }
    val args = classArgs ++ confArgs ++ Seq(req.appResource) ++
      Option(req.appArgs).map(_.toSeq).getOrElse(Seq.empty)
    val sa = new SparkSubmitArguments(args, sys.env)
    val (_, _, sparkConf, _) = new SparkSubmit().prepareSubmitEnvironment(sa)

    val driveropConf =
      if (anyActiveWorkerHasDriverOp()) Map(SPARK_DRIVER_RESOURCE_DRIVEROP_AMOUNT -> "1")
      else Map.empty[String, String]
    val conf = masterConfSnapshot ++ readRemoteConfs() ++
      sparkConf.getAll.toMap ++ driveropConf ++ serverDerivedSparkProperties

    val out = new CreateSubmissionRequest()
    out.appResource = req.appResource
    out.mainClass = sa.mainClass
    out.appArgs = sa.childArgs.toArray
    out.sparkProperties = withIpv6Preference(conf)
    out.environmentVariables = Option(req.environmentVariables).getOrElse(Map.empty)
    out.clientSparkVersion = req.clientSparkVersion
    out
  }

  private[rest] def withIpv6Preference(conf: Map[String, String]): Map[String, String] = {
    if (!conf.get(YT_PREFERENCE_IPV6_ENABLED).exists(_.equalsIgnoreCase("true"))) return conf
    val withDriver = prependJavaOpt(conf, "spark.driver.extraJavaOptions")
    if (SparkVersionUtils.lessThan("3.4.0")) prependJavaOpt(withDriver, "spark.executor.extraJavaOptions") else withDriver
  }

  private lazy val masterConfSnapshot: Map[String, String] = masterConf.getAll.toMap
  private lazy val spytClusterVersion: String = sys.env.getOrElse(SPYT_CLUSTER_VERSION_ENV, "")
  private lazy val sparkBaseDiscoveryPath: String = sys.env.getOrElse(SPARK_BASE_DISCOVERY_PATH_ENV, "")

  private def readRemoteConfs(): Map[String, String] = try {
    val yt = YtClientProvider.ytClient(ytClientConfiguration(masterConf))
    safeRead(remoteGlobalConfigPath, yt) ++
      (if (spytClusterVersion.nonEmpty) safeRead(remoteVersionConfigPath(spytClusterVersion), yt) else Map.empty) ++
      (if (sparkBaseDiscoveryPath.nonEmpty) safeRead(remoteClusterConfigPath(sparkBaseDiscoveryPath), yt) else Map.empty)
  } catch {
    case NonFatal(e) =>
      logWarning("Failed to read remote SPYT configs, proceeding without them", e)
      Map.empty
  }

  private def safeRead(path: String, yt: tech.ytsaurus.client.CompoundClient): Map[String, String] = {
    try {
      parseRemoteConfig(path, yt).asScala.toMap
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to read remote SPYT config from $path", e)
        Map.empty
    }
  }

  private def anyActiveWorkerHasDriverOp(): Boolean = try {
    val resp = masterEndpoint.askSync[DeployMessages.MasterStateResponse](DeployMessages.RequestMasterState)
    resp.workers.exists(w => w.isAlive() && w.resources.contains("driverop"))
  } catch { case NonFatal(_) => false }

  private lazy val serverDerivedSparkProperties: Map[String, String] = {
    val versionConf =
      if (spytClusterVersion.nonEmpty) Map("spark.yt.cluster.version" -> spytClusterVersion)
      else Map.empty[String, String]
    val eventLogConf =
      if (sparkBaseDiscoveryPath.nonEmpty)
        Map("spark.eventLog.dir" -> s"ytEventLog:/${CypressDiscoveryService.eventLogPath(sparkBaseDiscoveryPath)}")
      else Map.empty[String, String]
    versionConf ++ eventLogConf
  }
}

private object SpytSubmitRequestServlet {
  val YT_PREFERENCE_IPV6_ENABLED: String = "spark.hadoop.yt.preferenceIpv6.enabled"
  val SPARK_DRIVER_RESOURCE_DRIVEROP_AMOUNT: String = "spark.driver.resource.driverop.amount"
  val SPYT_CLUSTER_VERSION_ENV: String = "SPYT_CLUSTER_VERSION"
  val SPARK_BASE_DISCOVERY_PATH_ENV: String = "SPARK_BASE_DISCOVERY_PATH"
  val PreferIpv6Opt: String = "-Djava.net.preferIPv6Addresses=true"

  def prependJavaOpt(conf: Map[String, String], key: String): Map[String, String] = {
    val cur = conf.getOrElse(key, "")
    if (cur.contains("java.net.preferIPv6Addresses")) conf
    else conf + (key -> (if (cur.isEmpty) PreferIpv6Opt else s"$PreferIpv6Opt $cur"))
  }
}
