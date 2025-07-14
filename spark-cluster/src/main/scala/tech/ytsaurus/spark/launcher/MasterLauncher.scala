package tech.ytsaurus.spark.launcher

import com.codahale.metrics.MetricRegistry
import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.launcher.AdditionalMetricsSender.startAdditionalMetricsSenderIfDefined
import tech.ytsaurus.spark.launcher.rest.MasterWrapperLauncher
import tech.ytsaurus.spark.metrics.AdditionalMetrics
import tech.ytsaurus.spyt.wrapper.TcpProxyService
import tech.ytsaurus.spyt.wrapper.TcpProxyService.updateTcpAddress
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.discovery.{Address, SparkConfYsonable}

import java.net.URI
import scala.concurrent.duration._
import scala.language.postfixOps

object MasterLauncher extends App
  with VanillaLauncher
  with SparkLauncher
  with MasterWrapperLauncher {

  private val log = LoggerFactory.getLogger(getClass)
  private val instance = "master"

  val masterArgs = MasterLauncherArgs(args)

  import masterArgs._

  val autoscalerConf: Option[AutoScaler.Conf] = AutoScaler.Conf(sparkSystemProperties)
  val additionalMetrics: MetricRegistry = new MetricRegistry
  AdditionalMetrics.register(additionalMetrics, instance)

  withYtClient(ytConfig) { yt =>
    val masterGroupId = groupId.map(getFullGroupId)
    withCompoundDiscovery(masterGroupId, masterGroupId, Some(baseDiscoveryPath), Some(yt)) { discoveryService =>
      log.info("Used discovery service: " + discoveryService.toString)
      val tcpRouter = TcpProxyService().register("HOST", "WEBUI", "REST", "WRAPPER")(yt)
      val reverseProxyUrl = tcpRouter.map(x => "http://" + x.getExternalAddress("WEBUI").toString)
      withService(startMaster(reverseProxyUrl)) { master =>
        withService(startMasterWrapper(args, master)) { masterWrapper =>
          master.waitAndThrowIfNotAlive(5 minutes)
          masterWrapper.waitAndThrowIfNotAlive(5 minutes)

          val masterAddress = tcpRouter.map { router =>
            val webUi = router.getExternalAddress("WEBUI")
            Address(
              router.getExternalAddress("HOST"),
              webUi,
              URI.create(s"http://$webUi"),
              router.getExternalAddress("REST")
            )
          }.getOrElse(master.masterAddress)
          val masterWrapperAddress =
            tcpRouter.map(_.getExternalAddress("WRAPPER")).getOrElse(masterWrapper.address)
          log.info("Register master")
          discoveryService.registerMaster(
            operationId,
            masterAddress,
            clusterVersion,
            masterWrapperAddress,
            SparkConfYsonable(sparkSystemProperties)
          )
          log.info("Master registered")
          tcpRouter.foreach { router =>
            updateTcpAddress(master.masterAddress.hostAndPort.toString, router.getPort("HOST"))(yt)
            updateTcpAddress(master.masterAddress.webUiHostAndPort.toString, router.getPort("WEBUI"))(yt)
            updateTcpAddress(master.masterAddress.restHostAndPort.toString, router.getPort("REST"))(yt)
            updateTcpAddress(masterWrapper.address.toString, router.getPort("WRAPPER"))(yt)
            log.info("Tcp proxy port addresses updated")
          }

          autoscalerConf foreach { conf =>
            AutoScaler.start(AutoScaler.build(conf, discoveryService, yt), conf, additionalMetrics)
          }

          startAdditionalMetricsSenderIfDefined(sparkSystemProperties, spytHome, instance, additionalMetrics)

          def isAlive: Boolean = {
            val isMasterAlive = master.isAlive(processCheckRetries)

            val res = isMasterAlive
            if (res) {
              discoveryService.updateMaster(
                operationId,
                masterAddress,
                clusterVersion,
                masterWrapperAddress,
                SparkConfYsonable(sparkSystemProperties)
              )
            }
            res
          }

          checkPeriodically(isAlive)
          log.error("Master is not alive")
        }
      }
    }
  }
}

case class MasterLauncherArgs(ytConfig: YtClientConfiguration,
                              baseDiscoveryPath: String,
                              operationId: String,
                              groupId: Option[String])

object MasterLauncherArgs {
  def apply(args: Args): MasterLauncherArgs = MasterLauncherArgs(
    YtClientConfiguration(args.optional),
    args.optional("base-discovery-path").getOrElse(sys.env("SPARK_BASE_DISCOVERY_PATH")),
    args.optional("operation-id").getOrElse(sys.env("YT_OPERATION_ID")),
    args.optional("discovery-group-id").orElse(sys.env.get("SPARK_DISCOVERY_GROUP_ID")),
  )

  def apply(args: Array[String]): MasterLauncherArgs = MasterLauncherArgs(Args(args))
}
