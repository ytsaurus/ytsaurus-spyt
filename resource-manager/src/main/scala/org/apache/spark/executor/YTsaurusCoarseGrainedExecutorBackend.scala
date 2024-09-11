
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
        SparkAdapter.instance.executorBackendFactory
          .createExecutorBackend(rpcEnv, arguments, env, resourceProfile, ytTaskJobIndex)
    }
    val backendArgs = CoarseGrainedExecutorBackend.parseArguments(args,
      this.getClass.getCanonicalName.stripSuffix("$"))
    CoarseGrainedExecutorBackend.run(backendArgs, createFn)
    System.exit(0)
  }
}