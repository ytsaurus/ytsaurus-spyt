package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.SparkSession
import tech.ytsaurus.client.ApiServiceTransaction
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Streaming
import tech.ytsaurus.spyt.streaming.YtStreamingTransactionContext
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

  override def setTransactionId(txId: String): Unit = YtStreamingTransactionContext.setTransactionId(txId)

  override def clearTransactionId(): Unit = YtStreamingTransactionContext.clearTransactionId()
}

class YtStreamingTransactionHandle(transaction: ApiServiceTransaction) extends StreamingTransactionHandle {
  override def getId: String = transaction.getId.toString

  override def commit(): Unit = transaction.commit().join()

  override def abort(): Unit = transaction.abort().join()
}
