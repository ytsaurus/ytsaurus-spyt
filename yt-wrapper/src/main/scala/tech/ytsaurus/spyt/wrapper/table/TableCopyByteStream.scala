package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.client.TableReader

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.concurrent.duration.Duration


class TableCopyByteStream(reader: TableReader[ByteBuffer], timeout: Duration,
                          reportBytesRead: Long => Unit) extends YtArrowInputStream {
  private var _batch = ByteBuffer.allocate(0)
  private val nextPageToken = ByteBuffer.wrap(Array(-1, -1, -1, -1, 0, 0, 0, 0))
  private val emptySchemaToken = ByteBuffer.wrap(Array(0, 0, 0, 0, 0, 0, 0, 0))

  override def read(): Int = ???

  override def read(b: Array[Byte]): Int = {
    read(b, 0, b.length)
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    read(b, off, len, 0)
  }

  private def recognizeToken(token: ByteBuffer): Boolean = {
    if (hasNext) {
      val mismatch = _batch.mismatch(token)
      if (mismatch == -1 || mismatch == token.remaining()) {
        _batch.position(_batch.position() + token.remaining())
        return true
      }
    }
    false
  }

  override def isNextPage: Boolean = {
    recognizeToken(nextPageToken)
  }

  override def isEmptyPage: Boolean = {
    recognizeToken(emptySchemaToken)
  }

  @tailrec
  private def read(b: Array[Byte], off: Int, len: Int, readLen: Int): Int = len match {
    case 0 => readLen
    case _ =>
      if (hasNext) {
        val readBytes = Math.min(len, _batch.remaining())
        readFromBatch(b, off, readBytes)
        read(b, off + readBytes, len - readBytes, readLen + readBytes)
      } else readLen
  }

  private def hasNext: Boolean = {
    _batch.hasRemaining || readNextBatch()
  }

  private def readFromBatch(b: Array[Byte], off: Int, len: Int): Unit = {
    _batch.get(b, off, len)
    reportBytesRead(len)
  }

  private def readNextBatch(): Boolean = {
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
