
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.SparkContext
import org.apache.spark.deploy.ytsaurus.Config._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.Utils
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.TcpProxyService

private[spark] class YTsaurusSchedulerBackend (
  scheduler: TaskSchedulerImpl,
  sc: SparkContext,
  operationManager: YTsaurusOperationManager
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private val defaultProfile = scheduler.sc.resourceProfileManager.defaultResourceProfile
  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  def initialize(): Unit = {
    sc.uiWebUrl.foreach { internalWebUiUrl =>
      sys.env.get("YT_OPERATION_ID").foreach { ytOperationId =>
        val webUiUrl = if (sc.conf.get(TCP_PROXY_ENABLED)) {
          implicit val ytClient: CompoundClient = operationManager.ytClient
          val tcpRouterOpt = TcpProxyService(
            sc.conf.get(TCP_PROXY_ENABLED),
            sc.conf.get(TCP_PROXY_RANGE_START),
            sc.conf.get(TCP_PROXY_RANGE_SIZE)
          ).register("DRIVER")

          tcpRouterOpt.foreach { tcpRouter =>
            val address = internalWebUiUrl.replace("http://", "").replace("https://", "")
            TcpProxyService.updateTcpAddress(address, tcpRouter.getPort("DRIVER"))
          }

          tcpRouterOpt.map { tcpRouter =>
            "http://" + tcpRouter.getExternalAddress("DRIVER").toString
          }.get
        } else {
          internalWebUiUrl
        }
        val operation = YTsaurusOperation(GUID.valueOf(ytOperationId))
        operationManager.setOperationDescription(operation, Map(YTsaurusOperationManager.WEB_UI_KEY -> webUiUrl))
      }
    }
  }

  override def start(): Unit = {
    super.start()
    operationManager.startExecutors(sc, applicationId(), defaultProfile, initialExecutors)
  }

  override def stop(): Unit = {
    logInfo("Stopping YTsaurusSchedulerBackend")
    Utils.tryLogNonFatalError {
      super.stop()
    }

    Utils.tryLogNonFatalError {
      Thread.sleep(conf.get(EXECUTOR_OPERATION_SHUTDOWN_DELAY))
      operationManager.stopExecutors(sc)
    }

    Utils.tryLogNonFatalError {
      operationManager.close()
    }
  }

  override def applicationId(): String = {
    conf.getOption("spark.app.id").getOrElse(super.applicationId())
  }
}
