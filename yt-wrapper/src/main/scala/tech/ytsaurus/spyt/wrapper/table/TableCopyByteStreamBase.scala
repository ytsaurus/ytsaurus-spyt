package tech.ytsaurus.spyt.wrapper.table

import java.nio.ByteBuffer
import scala.annotation.tailrec

abstract class TableCopyByteStreamBase extends YtArrowInputStream {
  protected var _batch: ByteBuffer = ByteBuffer.allocate(0)
  private val nextPageToken: ByteBuffer = ByteBuffer.wrap(Array(-1, -1, -1, -1, 0, 0, 0, 0))
  private val emptySchemaToken: ByteBuffer = ByteBuffer.wrap(Array(0, 0, 0, 0, 0, 0, 0, 0))
  protected val reportBytesRead: Long => Unit

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

  protected def readFromBatch(b: Array[Byte], off: Int, len: Int): Unit = {
    _batch.get(b, off, len)
    reportBytesRead(len)
  }

  protected def readNextBatch(): Boolean

  override def close(): Unit
}
