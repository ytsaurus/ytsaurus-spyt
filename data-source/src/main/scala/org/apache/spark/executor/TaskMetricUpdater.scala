package org.apache.spark.executor

import org.apache.spark.TaskContext

object TaskMetricUpdater {
  // NB: Call only on a executor.
  def reportBytesWritten(taskContext: TaskContext, bytesWritten: Long): Unit = {
    Option(taskContext).foreach { task =>
      task.taskMetrics().outputMetrics._bytesWritten.add(bytesWritten)
    }
  }
}
