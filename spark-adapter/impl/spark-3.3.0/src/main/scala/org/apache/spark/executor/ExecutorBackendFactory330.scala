package org.apache.spark.executor

import org.apache.spark.SparkEnv
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

object ExecutorBackendFactory330 extends ExecutorBackendFactory {

  override def createExecutorBackend(rpcEnv: RpcEnv,
                                     arguments: CoarseGrainedExecutorBackend.Arguments,
                                     env: SparkEnv,
                                     resourceProfile: ResourceProfile,
                                     ytTaskJobIndex: String): CoarseGrainedExecutorBackend = {
    new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl,
      ytTaskJobIndex, arguments.bindAddress, arguments.hostname, arguments.cores,
      env, arguments.resourcesFileOpt, resourceProfile)
  }
}
