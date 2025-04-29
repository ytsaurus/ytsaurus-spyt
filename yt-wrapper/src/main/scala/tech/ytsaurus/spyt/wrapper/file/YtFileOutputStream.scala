package tech.ytsaurus.spyt.wrapper.file

import tech.ytsaurus.spyt.wrapper.client.YtRpcClient
import tech.ytsaurus.client.FileWriter

import java.io.OutputStream
import scala.annotation.tailrec

class YtFileOutputStream(writer: FileWriter, yt: Option[YtRpcClient]) extends OutputStream {
  private var closed = false

  override def write(b: Int): Unit = {
    write(Array(b.toByte), 0, 1)
  }

  override final def write(b: Array[Byte], off: Int, len: Int): Unit = {
    recursiveWrite(b, off, len)
  }

  @tailrec
  private def recursiveWrite(b: Array[Byte], off: Int, len: Int): Unit = {
    if (!closed && !writer.write(b, off, len)) {
      writer.readyEvent().join()
      recursiveWrite(b, off, len)
    }
  }

  override def write(b: Array[Byte]): Unit = {
    write(b, 0, b.length)
  }

  override def flush(): Unit = {
  }

  override def close(): Unit = {
    if (!closed) {
      try {
        writer.readyEvent().join()
        writer.close().join()
      } finally {
        closed = true
      }
    }
  }
}
