package tech.ytsaurus.spyt.common.utils

import org.apache.spark.unsafe.types.UTF8String

import java.nio.ByteBuffer

object UuidUtils {
  private val UUID_STRING_LENGTH = 36
  private val UUID_BYTES_LENGTH = 16
  private val HYPHEN_POSITIONS = Set(8, 13, 18, 23)

  def UTF8UuidToBytes(uuid: UTF8String): Array[Byte] = {
    uuidToBytes(uuid.toString)
  }

  def uuidToBytes(uuid: String): Array[Byte] = {
    validateFormat(uuid)

    val bb = ByteBuffer.allocate(UUID_BYTES_LENGTH)
    parseComponent(uuid, 0, 8, bb, reverse = true)      // 4-byte time_low (little-endian → big-endian)
    parseComponent(uuid, 9, 13, bb, reverse = true)     // 2-byte time_mid (little-endian → big-endian)
    parseComponent(uuid, 14, 18, bb, reverse = true)    // 2-byte time_high (little-endian → big-endian)
    parseComponent(uuid, 19, 23, bb, reverse = false)   // 2-byte clock_seq (unchanged)
    parseComponent(uuid, 24, 36, bb, reverse = false)   // 6-byte node (unchanged)

    bb.array()
  }

  def bytesToUTF8Uuid(bytes: Array[Byte]): UTF8String = {
    UTF8String.fromString(bytesToUuid(bytes))
  }

  def bytesToUuid(bytes: Array[Byte]): String = {
    require(bytes.length == UUID_BYTES_LENGTH, s"UUID must be $UUID_BYTES_LENGTH bytes")

    val bb = ByteBuffer.wrap(bytes)
    val components = Seq(
      readComponent(bb, 4, reverse = true),  // time_low
      readComponent(bb, 2, reverse = true),  // time_mid
      readComponent(bb, 2, reverse = true),  // time_high
      readComponent(bb, 2, reverse = false), // clock_seq
      readComponent(bb, 6, reverse = false)  // node
    )

    components.mkString("-")
  }

  private def parseComponent( s: String,
                              start: Int,
                              end: Int,
                              bb: ByteBuffer,
                              reverse: Boolean
                            ): Unit = {
    val pairs = (0 until (end-start)/2).map { i =>
      val offset = start + i * 2
      s.substring(offset, offset + 2)
    }

    val orderedPairs = if (reverse) pairs.reverse else pairs

    orderedPairs.foreach { pair =>
      bb.put(parseHexPair(pair))
    }
  }

  private def readComponent(bb: ByteBuffer, byteCount: Int, reverse: Boolean): String = {
    val buffer = new Array[Byte](byteCount)
    bb.get(buffer)
    val ordered = if (reverse) buffer.reverse else buffer
    bytesToHex(ordered)
  }

  private def bytesToHex(bytes: Array[Byte]): String = {
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }

  private def parseHexPair(pair: String): Byte = {
    val high = charToNibble(pair.charAt(0))
    val low = charToNibble(pair.charAt(1))
    ((high << 4) | low).toByte
  }

  private def charToNibble(c: Char): Int = c match {
    case _ if c >= '0' && c <= '9' => c - '0'
    case _ if c >= 'a' && c <= 'f' => c - 'a' + 10
    case _ if c >= 'A' && c <= 'F' => c - 'A' + 10
    case _ => throw new IllegalArgumentException(s"Invalid hex character: $c")
  }

  private def validateFormat(s: String): Unit = {
    require(s.length == UUID_STRING_LENGTH,
      s"Invalid UUID length: ${s.length}, expected $UUID_STRING_LENGTH")

    HYPHEN_POSITIONS.foreach { pos =>
      require(s.charAt(pos) == '-',
        s"Missing hyphen at position $pos in UUID string")
    }
  }
}
