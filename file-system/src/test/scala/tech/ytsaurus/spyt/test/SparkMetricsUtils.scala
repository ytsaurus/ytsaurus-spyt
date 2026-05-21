package tech.ytsaurus.spyt.test

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicLong

/**
 * Shared utilities for collecting Spark task-level I/O metrics in tests.
 */
trait SparkMetricsUtils {
  def withMetrics[T](spark: SparkSession)(
    action: SparkMetricsUtils.BytesMetricsListener => T
  ): T = {
    val listener = new SparkMetricsUtils.BytesMetricsListener()
    spark.sparkContext.addSparkListener(listener)

    try {
      action(listener)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}

object SparkMetricsUtils {

  final case class BytesMetrics(
    bytesWritten: Long,
    bytesRead: Long,
    recordsWritten: Long,
    recordsRead: Long
  )

  final class BytesMetricsListener extends SparkListener {
    private val bytesWrittenCounter = new AtomicLong(0L)
    private val bytesReadCounter = new AtomicLong(0L)
    private val recordsWrittenCounter = new AtomicLong(0L)
    private val recordsReadCounter = new AtomicLong(0L)

    def bytesWritten: Long = bytesWrittenCounter.get()
    def bytesRead: Long = bytesReadCounter.get()
    def recordsWritten: Long = recordsWrittenCounter.get()
    def recordsRead: Long = recordsReadCounter.get()

    def snapshot(): BytesMetrics = {
      BytesMetrics(
        bytesWritten = bytesWritten,
        bytesRead = bytesRead,
        recordsWritten = recordsWritten,
        recordsRead = recordsRead
      )
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val taskMetrics = taskEnd.taskMetrics

      if (taskMetrics != null) {
        val outputMetrics = taskMetrics.outputMetrics
        if (outputMetrics != null) {
          bytesWrittenCounter.addAndGet(outputMetrics.bytesWritten)
          recordsWrittenCounter.addAndGet(outputMetrics.recordsWritten)
        }

        val inputMetrics = taskMetrics.inputMetrics
        if (inputMetrics != null) {
          bytesReadCounter.addAndGet(inputMetrics.bytesRead)
          recordsReadCounter.addAndGet(inputMetrics.recordsRead)
        }
      }
    }

    def reset(): Unit = {
      bytesWrittenCounter.set(0L)
      bytesReadCounter.set(0L)
      recordsWrittenCounter.set(0L)
      recordsReadCounter.set(0L)
    }
  }
}
