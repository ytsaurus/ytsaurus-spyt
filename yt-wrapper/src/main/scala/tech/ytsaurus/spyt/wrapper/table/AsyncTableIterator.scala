package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.client.AsyncReader

// TODO (mihailagei): Add using reportBytesRead when TRspReadTablePartitionMeta will contain statistics
//  similar to those in TRspReadTableMeta
class AsyncTableIterator[T](reader: AsyncReader[T], reportBytesRead: Long => Unit) extends TableIterator[T] {
  private var currentBatch: java.util.Iterator[T] = _
  private var isExhausted: Boolean = false

  override def hasNext: Boolean = {
    if (currentBatch != null && currentBatch.hasNext) {
      true
    } else if (!isExhausted) {
      readNextBatch()
    } else {
      close()
      false
    }
  }

  private def readNextBatch(): Boolean = {
    val list = reader.next().join()
    if (list != null) {
      currentBatch = list.iterator
      currentBatch.hasNext || hasNext
    } else {
      isExhausted = true
      close()
      false
    }
  }

  override def next(): T = currentBatch.next()

  override def close(): Unit = reader.close()
}
