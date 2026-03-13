package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.SparkSession

import java.util.ServiceLoader

trait StreamingTransactionHandle {
  def getId: String

  def commit(): Unit

  def abort(): Unit
}

trait StreamingTransactionSupport {
  def isTransactionalStreamingEnabled(sparkSession: SparkSession): Boolean

  def createTransaction(sparkSession: SparkSession): StreamingTransactionHandle

  def setTransactionId(txId: String): Unit

  def clearTransactionId(): Unit
}

object StreamingTransactionSupport {
  lazy val instance: StreamingTransactionSupport = ServiceLoader.load(classOf[StreamingTransactionSupport]).findFirst().get()
}
