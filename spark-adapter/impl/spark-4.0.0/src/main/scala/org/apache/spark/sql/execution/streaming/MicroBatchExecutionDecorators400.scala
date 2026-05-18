package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.classic.SparkSession
import tech.ytsaurus.spyt.adapter.{StreamingTransactionHandle, StreamingTransactionSupport}
import tech.ytsaurus.spyt.logging.Logging
import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass, PatchSource}

import scala.util.control.NonFatal

@Decorate
@OriginClass("org.apache.spark.sql.execution.streaming.MicroBatchExecution")
@Applicability(from = "4.0.0", to = "4.1.0")
class MicroBatchExecutionDecorators400 extends Logging {

  var commitLog: CommitLog = ???

  @DecoratedMethod
  private def runBatch(execCtx: MicroBatchExecutionContext, sparkSessionToRunBatch: SparkSession): Unit = {
    val sts = StreamingTransactionSupport.instance
    val transactionalStreamingEnabled = sts.isTransactionalStreamingEnabled(sparkSessionToRunBatch)

    if (!transactionalStreamingEnabled) {
      __runBatch(execCtx, sparkSessionToRunBatch)
      return
    }

    if (sts.isRecoveryNeeded) {
      logWarning("Transactional streaming entering batch with recoveryNeeded flag set. " +
        "Previous batch failed and the YTsaurus consumer state may diverge from the Spark commit log.")
    }

    val currentTransaction: StreamingTransactionHandle = sts.createTransaction(sparkSessionToRunBatch)
    sts.setTransaction(currentTransaction)
    val batchIdAtEntry = execCtx.batchId
    var commitLogWritten = false

    try {
      __runBatch(execCtx, sparkSessionToRunBatch)
      commitLogWritten = true
      MicroBatchExecutionDecorators400.commitOffsets(execCtx.endOffsets)
      currentTransaction.commit()
      sts.clearRecoveryNeeded()
    } catch {
      case e: Exception =>
        sts.markRecoveryNeeded()
        try { currentTransaction.abort() } catch { case NonFatal(_) => () }
        if (commitLogWritten) {
          MicroBatchExecutionDecorators400.deleteCommitLogEntry(commitLog, batchIdAtEntry)
        }
        throw e
    } finally {
      sts.clearTransactionId()
    }
  }

  private def __runBatch( execCtx: MicroBatchExecutionContext, sparkSessionToRunBatch: SparkSession): Unit = ???
}

object MicroBatchExecutionDecorators400 extends Logging {
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
