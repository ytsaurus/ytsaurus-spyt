package tech.ytsaurus.spyt.adapter

import org.apache.spark.sql.SparkSession

import java.util.ServiceLoader

trait StreamingTransactionHandle {
  def getId: String

  def getStickyProxyAddress: Option[String] = None

  def commit(): Unit

  def abort(): Unit
}

trait StreamingTransactionSupport {
  def isTransactionalStreamingEnabled(sparkSession: SparkSession): Boolean

  def createTransaction(sparkSession: SparkSession): StreamingTransactionHandle

  def setTransaction(handle: StreamingTransactionHandle): Unit = {
    setTransactionId(handle.getId)
    setStickyProxyAddress(handle.getStickyProxyAddress)
  }

  def setTransactionId(txId: String): Unit

  def setStickyProxyAddress(address: Option[String]): Unit = {}

  def clearTransactionId(): Unit

  def isRecoveryNeeded: Boolean = false

  def markRecoveryNeeded(): Unit = {}

  def clearRecoveryNeeded(): Unit = {}
}

object StreamingTransactionSupport {
  lazy val instance: StreamingTransactionSupport = ServiceLoader.load(classOf[StreamingTransactionSupport]).findFirst().get()
}
