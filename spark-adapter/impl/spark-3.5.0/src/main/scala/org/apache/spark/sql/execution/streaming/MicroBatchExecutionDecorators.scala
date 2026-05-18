package org.apache.spark.sql.execution.streaming

import tech.ytsaurus.spyt.logging.Logging
import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.adapter.{StreamingTransactionHandle, StreamingTransactionSupport}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

import scala.util.control.NonFatal

@Decorate
@OriginClass("org.apache.spark.sql.execution.streaming.MicroBatchExecution")
@Applicability(from = "3.5.0", to = "4.0.0")
class MicroBatchExecutionDecorators extends Logging {

  var availableOffsets: StreamProgress = ???
  var commitLog: CommitLog = ???
  var currentBatchId: Long = ???

  @DecoratedMethod
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    val sts = StreamingTransactionSupport.instance
    val transactionalStreamingEnabled = sts.isTransactionalStreamingEnabled(sparkSessionToRunBatch)

    if (!transactionalStreamingEnabled) {
      __runBatch(sparkSessionToRunBatch)
      return
    }

    if (sts.isRecoveryNeeded) {
      logWarning("Transactional streaming entering batch with recoveryNeeded flag set. " +
        "Previous batch failed and the YTsaurus consumer state may diverge from the Spark commit log.")
    }

    val currentTransaction: StreamingTransactionHandle = sts.createTransaction(sparkSessionToRunBatch)
    sts.setTransaction(currentTransaction)
    val batchIdAtEntry = currentBatchId
    var commitLogWritten = false

    try {
      __runBatch(sparkSessionToRunBatch)
      commitLogWritten = true
      MicroBatchExecutionDecorators.commitOffsets(availableOffsets)
      currentTransaction.commit()
      sts.clearRecoveryNeeded()
    } catch {
      case e: Exception =>
        sts.markRecoveryNeeded()
        try { currentTransaction.abort() } catch { case NonFatal(_) => () }
        if (commitLogWritten) {
          MicroBatchExecutionDecorators.deleteCommitLogEntry(commitLog, batchIdAtEntry)
        }
        throw e
    } finally {
      sts.clearTransactionId()
    }
  }

  private def __runBatch(sparkSessionToRunBatch: SparkSession): Unit = ???
}

object MicroBatchExecutionDecorators extends Logging {
  def commitOffsets(availableOffsets: StreamProgress): Unit = {
    for ((src, offset) <- availableOffsets.toList) {
      src match {
        case source: Source => source.commit(offset.asInstanceOf[Offset])
        case _ =>
      }
    }
  }

  def deleteCommitLogEntry(commitLog: CommitLog, batchId: Long): Unit = {
    try {
      commitLog.purgeAfter(batchId - 1)
      logWarning(s"Rolled back commit log entry $batchId because YT transaction commit failed")
    } catch {
      case t: Throwable =>
        logWarning(s"Failed to roll back commit log entry $batchId: ${t.getClass.getName}: ${t.getMessage}", t)
    }
  }
}
