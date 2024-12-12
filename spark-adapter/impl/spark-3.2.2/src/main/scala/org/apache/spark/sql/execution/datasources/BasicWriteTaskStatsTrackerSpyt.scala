package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

import scala.collection.mutable


@Subclass
@OriginClass("org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker")
class BasicWriteTaskStatsTrackerSpyt(hadoopConf: Configuration, taskCommitTimeMetric: Option[SQLMetric] = None)
  extends BasicWriteTaskStatsTracker(hadoopConf, taskCommitTimeMetric) {

  private val submittedFiles = getSuperField("submittedFiles").asInstanceOf[mutable.HashSet[String]]
  private val updateFileStatsS = this.getClass.getSuperclass.getDeclaredMethod("updateFileStats", classOf[String])

  private def getSuperField(name: String): AnyRef = {
    val field = this.getClass.getSuperclass.getDeclaredField(name)
    field.setAccessible(true)
    val result = field.get(this)
    field.setAccessible(false)
    result
  }

  override def closeFile(filePath: String): Unit = {
    updateFileStats(filePath)
    submittedFiles.remove(filePath)
  }

  private def updateFileStats(filePath: String): Unit = {
    if (filePath.startsWith("ytTable:/")) return

    updateFileStatsS.setAccessible(true)
    updateFileStatsS.invoke(this, filePath).asInstanceOf[Option[Long]]
    updateFileStatsS.setAccessible(false)
  }

  override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
    submittedFiles.foreach(updateFileStats)
    submittedFiles.clear()

    val partitions = getSuperField("partitions").asInstanceOf[mutable.ArrayBuffer[InternalRow]]
    val numFiles = getSuperField("numFiles").asInstanceOf[Int]
    var numBytes = getSuperField("numBytes").asInstanceOf[Long]
    val numRows = getSuperField("numRows").asInstanceOf[Long]

    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { metrics =>
      numBytes += metrics.bytesWritten
      metrics.setBytesWritten(numBytes)
      metrics.setRecordsWritten(numRows)
    }

    taskCommitTimeMetric.foreach(_ += taskCommitTime)
    BasicWriteTaskStats(partitions, numFiles, numBytes, numRows)
  }
}
