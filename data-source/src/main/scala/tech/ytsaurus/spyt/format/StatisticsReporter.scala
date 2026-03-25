package tech.ytsaurus.spyt.format

import org.apache.spark.TaskContext
import org.apache.spark.executor.TaskMetricUpdater
import tech.ytsaurus.client.rpc.RpcRequestDescriptor
import tech.ytsaurus.core.GUID
import tech.ytsaurus.spyt.wrapper.client.SpytRpcClientListener

import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable

object StatisticsReporter extends SpytRpcClientListener {

  override val id: String = "StatisticsReporter"

  private val registeredRequests = mutable.HashMap[GUID, TaskContext]()
  private val pendingBytesRead = mutable.HashMap[GUID, LongAdder]()

  private val writeMethods = Set(
    "WriteTable",
    "WriteTableFragment"
  )

  private val readMethods = Set(
    "ReadTablePartition",
    "ReadTable"
  )

  override def onBytesSent(context: RpcRequestDescriptor, bytes: Long): Unit = {
    if (writeMethods.contains(context.getMethod) && context.isStream && registeredRequests.contains(context.getRequestId)) {
      val taskContext = registeredRequests(context.getRequestId)
      TaskMetricUpdater.reportBytesWritten(taskContext, bytes)
    }
  }

  override def onBytesReceived(context: RpcRequestDescriptor, bytes: Long): Unit = {
    if (readMethods.contains(context.getMethod) && registeredRequests.contains(context.getRequestId)) {
      pendingBytesRead.getOrElseUpdate(context.getRequestId, new LongAdder()).add(bytes)
    }
  }

  def registerRequest(requestId: GUID, taskContext: TaskContext): Unit = {
    registeredRequests.put(requestId, taskContext)
  }

  def unregisterRequest(requestId: GUID): Unit = {
    registeredRequests.remove(requestId)
  }

  def registerReadMetrics(requestId: GUID, reportBytesRead: Long => Unit): Unit = {
    val taskContext = TaskContext.get()
    registerRequest(requestId, taskContext)
    taskContext.addTaskCompletionListener[Unit] { _ =>
      val bytesRead = pendingBytesRead.remove(requestId).map(_.longValue()).getOrElse(0L)
      unregisterRequest(requestId)
      reportBytesRead(bytesRead)
    }
  }
}
