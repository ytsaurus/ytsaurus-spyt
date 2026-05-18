
package org.apache.spark.executor

import org.apache.spark.SparkEnv
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv
import tech.ytsaurus.spyt.SparkAdapter

object YTsaurusCoarseGrainedExecutorBackend {
  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, CoarseGrainedExecutorBackend.Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, arguments, env, resourceProfile) =>
        val ytTaskJobIndex = System.getenv("YT_TASK_JOB_INDEX")
        new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl,
          ytTaskJobIndex, arguments.bindAddress, arguments.hostname, arguments.cores,
          env, arguments.resourcesFileOpt, resourceProfile)
    }
    val backendArgs = CoarseGrainedExecutorBackend.parseArguments(args,
      this.getClass.getCanonicalName.stripSuffix("$"))
    CoarseGrainedExecutorBackend.run(backendArgs, createFn)
    System.exit(0)
  }
}