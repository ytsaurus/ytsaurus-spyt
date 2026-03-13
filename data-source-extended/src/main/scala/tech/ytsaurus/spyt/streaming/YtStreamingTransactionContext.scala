package tech.ytsaurus.spyt.streaming

object YtStreamingTransactionContext {
  val currentTransactionId: ThreadLocal[Option[String]] = new ThreadLocal[Option[String]]() {
    override def initialValue(): Option[String] = None
  }

  def setTransactionId(txId: String): Unit = currentTransactionId.set(Some(txId))

  def clearTransactionId(): Unit = currentTransactionId.remove()
}
