package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.SparkConf
import org.apache.spark.deploy.ytsaurus.Config
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager.patchOperation

import tech.ytsaurus.client.YTsaurusClient
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.YtWrapper


class YTJobsExecutorAllocator(conf: SparkConf,
                              ytClient: YTsaurusClient) extends ExecutorAllocator with Logging {

  override def setTotalExpectedExecutors(resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Boolean = {
    // TODO: build per resource profile [SPYT-912]
    val totalExecs = resourceProfileToTotalExecs.values.sum
    logDebug(s"Requesting $totalExecs executors")

    conf.getOption(Config.EXECUTOR_OPERATION_ID) match {
      case None =>
        logWarning("No executor operation ID found in configuration")
        false

      case Some(opId) =>
        val operation = YTsaurusOperation(GUID.valueOf(opId))

        YtWrapper.fetchBriefProgress(operation.id)(ytClient) match {
          case None =>
            logWarning("No brief progress information found in operation spec")
            false

          case Some(bp) =>
            logDebug(s"Current operation job brief progress: ${bp}")
            val currentJobsCount = bp.running + bp.pending

            if (totalExecs <= currentJobsCount) {
              logDebug("No need to increase number of jobs: jobs will be completed when executors are decommissioned")
              true
            } else {
              val newTotalJobsCount = bp.total + (totalExecs - currentJobsCount)
              logDebug(s"Patching operation from ${bp.total} jobs up to $newTotalJobsCount jobs")
              patchOperation(operation, newTotalJobsCount, ytClient)
            }
        }
    }
  }
}
