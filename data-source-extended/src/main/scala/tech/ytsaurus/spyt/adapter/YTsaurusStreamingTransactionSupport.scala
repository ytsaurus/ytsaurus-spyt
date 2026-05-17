package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.ApiServiceTransaction
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Streaming
import tech.ytsaurus.spyt.streaming.{StreamingTxContext, YtStreamingTransactionContext}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfigurationConverter, YtClientProvider}
import tech.ytsaurus.spyt.wrapper.config.SparkYtSparkSession

import java.time.Duration

class YTsaurusStreamingTransactionSupport extends StreamingTransactionSupport {

  override def isTransactionalStreamingEnabled(sparkSession: SparkSession): Boolean = {
    sparkSession.ytConf(Streaming.Transactional)
  }

  override def createTransaction(sparkSession: SparkSession): StreamingTransactionHandle = {
    val ytClient = YtClientProvider.ytClient(
      YtClientConfigurationConverter.ytClientConfiguration(sparkSession.sparkContext.getConf)
    )
    val transaction = YtWrapper.createTransaction(
      parent = None,
      timeout = Duration.ofMinutes(5),
      sticky = true
    )(ytClient)

    new YtStreamingTransactionHandle(transaction)
  }

  override def setTransaction(handle: StreamingTransactionHandle): Unit =
    YtStreamingTransactionContext.setContext(StreamingTxContext(handle.getId, handle.getStickyProxyAddress))

  override def setTransactionId(txId: String): Unit = YtStreamingTransactionContext.setTransactionId(txId)

  override def setStickyProxyAddress(address: Option[String]): Unit =
    YtStreamingTransactionContext.setStickyProxyAddress(address)

  override def clearTransactionId(): Unit = YtStreamingTransactionContext.clearTransactionId()

  override def isRecoveryNeeded: Boolean = YtStreamingTransactionContext.isRecoveryNeeded

  override def markRecoveryNeeded(): Unit = YtStreamingTransactionContext.markRecoveryNeeded()

  override def clearRecoveryNeeded(): Unit = YtStreamingTransactionContext.clearRecoveryNeeded()
}

class YtStreamingTransactionHandle(transaction: ApiServiceTransaction) extends StreamingTransactionHandle {
  private val log = LoggerFactory.getLogger(getClass)

  override def getId: String = transaction.getId.toString

  override def getStickyProxyAddress: Option[String] = Option(transaction.getRpcProxyAddress)

  override def commit(): Unit = transaction.commit().join()

  override def abort(): Unit = {
    try {
      transaction.abort().join()
    } catch {
      case e: IllegalStateException =>
        log.warn("Transaction abort() failed with IllegalStateException: {}", e.getMessage)
    }
  }
}
