package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.client.AsyncReader

import java.nio.ByteBuffer

class PartitionCopyByteStream(reader: AsyncReader[ByteBuffer], override val reportBytesRead: Long => Unit)
  extends TableCopyByteStreamBase {

  override protected def readNextBatch(): Boolean = {
    val future = reader.next()
    val list = future.join()

    if (list != null && !list.isEmpty) {
      _batch = list.get(0)
      val bytes = new Array[Byte](_batch.remaining())
      val originalPosition = _batch.position()
      _batch.get(bytes)
      _batch.position(originalPosition)
      true
    } else {
      false
    }
  }

  override def close(): Unit = reader.close()
}