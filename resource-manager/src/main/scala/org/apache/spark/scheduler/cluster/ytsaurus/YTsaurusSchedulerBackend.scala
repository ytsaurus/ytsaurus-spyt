
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.SparkContext
import org.apache.spark.deploy.ytsaurus.Config._
import org.apache.spark.internal.config.SUBMIT_DEPLOY_MODE
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.{ExecutorDecommissionInfo, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.Utils
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.TcpProxyService

import scala.concurrent.Future

private[spark] class YTsaurusSchedulerBackend (
  scheduler: TaskSchedulerImpl,
  sc: SparkContext,
  operationManager: YTsaurusOperationManager
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  private val defaultProfile = scheduler.sc.resourceProfileManager.defaultResourceProfile
  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  private val executorsJobsAllocator = new YTJobsExecutorAllocator(conf, operationManager.ytClient)

  def initialize(): Unit = {
    if (sc.conf.get(SUBMIT_DEPLOY_MODE) == "cluster" && sc.uiWebUrl.isDefined) {
      val internalWebUiUrl = sc.uiWebUrl.get
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
    logInfo(s"Starting YTsaurusSchedulerBackend with initial executors count: $initialExecutors")
    val executorOperation = operationManager.startExecutors(sc, applicationId(), defaultProfile, initialExecutors)
    sc.conf.getOption(DRIVER_OPERATION_ID).foreach { driverOperationId =>
      val driverOperation = YTsaurusOperation(GUID.valueOf(driverOperationId))
      operationManager.setOperationDescription(
        driverOperation,
        Map(YTsaurusOperationManager.EXECUTORS_OPERATION_ID_KEY -> executorOperation.id.toString)
      )
    }
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

  protected override def doRequestTotalExecutors(resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    logInfo(s"Requesting ${resourceProfileToTotalExecs.values.sum} executor(s) from the cluster manager")
    val patchOperationState = executorsJobsAllocator.setTotalExpectedExecutors(resourceProfileToTotalExecs)
    Future.successful(patchOperationState)
  }

  override def decommissionExecutors(executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
                                     adjustTargetNumExecutors: Boolean,
                                     triggeredByExecutor: Boolean): Seq[String] = {
    logInfo("Decommissioning executors: " + executorsAndDecomInfo.map(_._1).mkString(", "))
    super.decommissionExecutors(executorsAndDecomInfo, adjustTargetNumExecutors, triggeredByExecutor)
  }
}
