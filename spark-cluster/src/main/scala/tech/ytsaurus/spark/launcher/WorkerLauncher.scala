package tech.ytsaurus.spark.launcher

import com.codahale.metrics.MetricRegistry
import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spark.launcher.ByopLauncher.ByopConfig
import tech.ytsaurus.spark.launcher.Service.LocalService
import tech.ytsaurus.spark.launcher.WorkerLogLauncher.WorkerLogConfig
import tech.ytsaurus.spark.metrics.AdditionalMetrics
import tech.ytsaurus.spyt.wrapper.Utils.parseDuration
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.discovery.DiscoveryService

import scala.concurrent.duration._
import scala.language.postfixOps

object WorkerLauncher extends App with VanillaLauncher with SparkLauncher with ByopLauncher with SolomonLauncher {
  private val log = LoggerFactory.getLogger(getClass)
  private val workerArgs = WorkerLauncherArgs(args)
  private val byopConfig = ByopConfig.create(sparkSystemProperties, args)
  private val workerLogConfig = WorkerLogConfig.create(sparkSystemProperties, args)
  private val additionalMetrics = new MetricRegistry
  AdditionalMetrics.register(additionalMetrics, "worker")

  import workerArgs._

  prepareProfiler()
  private val extraEnv = Map(
    "SPARK_JAVA_LOG4J_CONFIG" -> log4jConfigJavaOption(workerLogConfig.exists(_.enableJson))
  )


  def startWorkerLogService(client: CompoundClient): Option[Service] = {
    workerLogConfig.map(x => LocalService("WorkerLogService", WorkerLogLauncher.start(x, client)))
  }

  withOptionalService(byopConfig.map(startByop)) { byop =>
    withYtClient(ytConfig) { yt =>
      withCypressDiscovery(baseDiscoveryPath, yt) { cypressDiscovery =>
        withOptionalService(startWorkerLogService(yt)) { workerLog =>
          val masterAddress = waitForMaster(waitMasterTimeout, cypressDiscovery)
          cypressDiscovery.registerWorker(operationId)

          log.info(s"Starting worker for master $masterAddress")
          withService(startWorker(masterAddress, cores, memory, extraEnv, enableSquashfs)) { worker =>
            withOptionalService(startSolomonAgent(args, "worker", worker.address.port)) { solomonAgent =>
              def isAlive: Boolean = {
                val isMasterAlive = DiscoveryService.isAlive(masterAddress.webUiHostAndPort, 3)
                val isWorkerAlive = worker.isAlive(3)
                val isWorkerLogAlive = workerLog.forall(_.isAlive(3))
                val isRpcProxyAlive = byop.forall(_.isAlive(3))
                val isSolomonAgentAlive = solomonAgent.forall(_.isAlive(3))

                isMasterAlive && isWorkerAlive && isWorkerLogAlive && isRpcProxyAlive && isSolomonAgentAlive
              }

              if (solomonAgent.nonEmpty) {
                AdditionalMetricsSender(sparkSystemProperties, "worker", additionalMetrics).start()
              }
              checkPeriodically(isAlive)
            }
          }
        }
      }
    }
  }
}

case class WorkerLauncherArgs(cores: Int,
                              memory: String,
                              ytConfig: YtClientConfiguration,
                              baseDiscoveryPath: String,
                              waitMasterTimeout: Duration,
                              operationId: String,
                              enableSquashfs: Boolean
                             )

object WorkerLauncherArgs {
  def apply(args: Args): WorkerLauncherArgs = WorkerLauncherArgs(
    args.required("cores").toInt,
    args.required("memory"),
    YtClientConfiguration(args.optional),
    args.optional("base-discovery-path").getOrElse(sys.env("SPARK_BASE_DISCOVERY_PATH")),
    args.optional("wait-master-timeout").map(parseDuration).getOrElse(5 minutes),
    args.optional("operation-id").getOrElse(sys.env("YT_OPERATION_ID")),
    args.boolean("enable-squashfs")
  )

  def apply(args: Array[String]): WorkerLauncherArgs = WorkerLauncherArgs(Args(args))
}
