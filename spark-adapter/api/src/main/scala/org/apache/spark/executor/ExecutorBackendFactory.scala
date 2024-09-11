package org.apache.spark.executor

import org.apache.spark.SparkEnv
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

trait ExecutorBackendFactory {
  def createExecutorBackend(rpcEnv: RpcEnv,
                            arguments: CoarseGrainedExecutorBackend.Arguments,
                            env: SparkEnv,
                            resourceProfile: ResourceProfile,
                            ytTaskJobIndex: String): CoarseGrainedExecutorBackend

}
