package tech.ytsaurus.spyt.format

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetricUpdater
import tech.ytsaurus.client.rpc.RpcRequestDescriptor
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.client.SpytRpcClientListener

import scala.collection.mutable

object WriteStatisticsReporter extends SpytRpcClientListener {

  override val id: String = "WriteStatisticsReporter"

  private val registeredRequests = mutable.HashMap[GUID, TaskContext]()
  private val methods = Set(
    "WriteTable",
    "WriteTableFragment"
  )

  override def onBytesSent(context: RpcRequestDescriptor, bytes: Long): Unit = {
    if (methods.contains(context.getMethod) && context.isStream && registeredRequests.contains(context.getRequestId)) {
      val taskContext = registeredRequests(context.getRequestId)
      TaskMetricUpdater.reportBytesWritten(taskContext, bytes)
    }
  }

  def registerRequest(requestId: GUID, taskContext: TaskContext): Unit = {
    registeredRequests.put(requestId, taskContext)
  }

  def unregisterRequest(requestId: GUID): Unit = {
    registeredRequests.remove(requestId)
  }
}
