package tech.ytsaurus.spyt.streaming

import java.util.concurrent.atomic.AtomicBoolean

case class StreamingTxContext(txId: String, stickyAddress: Option[String])

object YtStreamingTransactionContext {
  private val currentTransactionContext: InheritableThreadLocal[Option[StreamingTxContext]] =
    new InheritableThreadLocal[Option[StreamingTxContext]]() {
      override def initialValue(): Option[StreamingTxContext] = None
    }

  private val recoveryNeededFlag: AtomicBoolean = new AtomicBoolean(false)

  def isRecoveryNeeded: Boolean = recoveryNeededFlag.get()

  def markRecoveryNeeded(): Unit = recoveryNeededFlag.set(true)

  def clearRecoveryNeeded(): Unit = recoveryNeededFlag.set(false)

  def get: Option[StreamingTxContext] = currentTransactionContext.get()

  def currentTransactionId: Option[String] = currentTransactionContext.get().map(_.txId)

  def currentStickyProxyAddress: Option[String] = currentTransactionContext.get().flatMap(_.stickyAddress)

  def setContext(ctx: StreamingTxContext): Unit = currentTransactionContext.set(Some(ctx))

  def setTransactionId(txId: String): Unit = {
    val sticky = currentTransactionContext.get().flatMap(_.stickyAddress)
    currentTransactionContext.set(Some(StreamingTxContext(txId, sticky)))
  }

  def setStickyProxyAddress(address: Option[String]): Unit = {
    currentTransactionContext.get() match {
      case Some(ctx) => currentTransactionContext.set(Some(ctx.copy(stickyAddress = address)))
      case None =>
    }
  }

  def clearTransactionId(): Unit = currentTransactionContext.remove()
}
