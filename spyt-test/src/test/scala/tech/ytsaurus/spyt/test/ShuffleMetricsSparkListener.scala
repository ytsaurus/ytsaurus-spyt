package tech.ytsaurus.spyt.test

import org.apache.spark.Success
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

/** Sums shuffle write and read metrics over all tasks of a session, so a test can compare the two sides. */
class ShuffleMetricsSparkListener extends SparkListener {

  private var writtenBytes: Long = 0L
  private var readBytes: Long = 0L
  private var fetchWaitMillis: Long = 0L
  private var spilledBytes: Long = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    // A retried task reports its write metrics again without a matching read, which would skew the comparison.
    if (metrics != null && taskEnd.reason == Success) {
      synchronized {
        writtenBytes += metrics.shuffleWriteMetrics.bytesWritten
        readBytes += metrics.shuffleReadMetrics.remoteBytesRead + metrics.shuffleReadMetrics.localBytesRead
        fetchWaitMillis += metrics.shuffleReadMetrics.fetchWaitTime
        spilledBytes += metrics.diskBytesSpilled
      }
    }
  }

  def shuffleBytesWritten: Long = synchronized(writtenBytes)

  def shuffleBytesRead: Long = synchronized(readBytes)

  def shuffleFetchWaitMillis: Long = synchronized(fetchWaitMillis)

  def diskBytesSpilled: Long = synchronized(spilledBytes)
}

object ShuffleMetricsSparkListener {

  def attachTo(spark: SparkSession): ShuffleMetricsSparkListener = {
    val listener = new ShuffleMetricsSparkListener
    spark.sparkContext.addSparkListener(listener)
    listener
  }
}
