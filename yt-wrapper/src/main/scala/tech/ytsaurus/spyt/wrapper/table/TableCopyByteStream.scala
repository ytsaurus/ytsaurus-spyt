package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.client.TableReader

import java.nio.ByteBuffer


class TableCopyByteStream(reader: TableReader[ByteBuffer], override val reportBytesRead: Long => Unit)
  extends TableCopyByteStreamBase {

  override protected def readNextBatch(): Boolean = {
    if (reader.canRead) {
      reader.readyEvent().join()
      val res = reader.read()
      if (res != null) {
        _batch = res.get(0)
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def close(): Unit = {
    if (reader.canRead) {
      reader.cancel()
    } else {
      reader.close().join()
    }
  }
}
