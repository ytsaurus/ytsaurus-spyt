package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.adapter.{StreamingTransactionHandle, StreamingTransactionSupport}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.execution.streaming.MicroBatchExecution")
class MicroBatchExecutionDecorators extends Logging {

  var availableOffsets: StreamProgress = ???

  @DecoratedMethod
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    val sts = StreamingTransactionSupport.instance
    val transactionalStreamingEnabled = sts.isTransactionalStreamingEnabled(sparkSessionToRunBatch)

    if (!transactionalStreamingEnabled) {
      __runBatch(sparkSessionToRunBatch)
      return
    }

    val currentTransaction: StreamingTransactionHandle = sts.createTransaction(sparkSessionToRunBatch)
    val currentTransactionId: String = currentTransaction.getId
    sts.setTransactionId(currentTransactionId)

    try {
      __runBatch(sparkSessionToRunBatch)
      MicroBatchExecutionDecorators.commitOffsets(availableOffsets)
      currentTransaction.commit()
    } catch {
      case e: Exception =>
        currentTransaction.abort()
        throw e
    } finally {
      sts.clearTransactionId()
    }
  }

  private def __runBatch(sparkSessionToRunBatch: SparkSession): Unit = ???
}

object MicroBatchExecutionDecorators {
  def commitOffsets(availableOffsets: StreamProgress): Unit = {
    for ((src, offset) <- availableOffsets.toList) {
      src match {
        case source: Source => source.commit(offset.asInstanceOf[Offset])
        case _ =>
      }
    }
  }
}
