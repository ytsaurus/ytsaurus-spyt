package tech.ytsaurus.spyt.serialization

import org.apache.spark.sql.spyt.types.{UInt64Long, UInt64Type}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExtendedYsonDecoderTest extends AnyFlatSpec with Matchers {

  it should "decode UInt64 values" in {
    val bytes = List( 6, 0x80, 0x80, 0x01).map(_.toByte).toArray

    val result: Long = YsonDecoder.decode(bytes, IndexedDataType.AtomicType(UInt64Type)).asInstanceOf[Long]

    UInt64Long(result) shouldEqual UInt64Long("16384")
  }

  it should "decode UInt64 values that doesn't fit into signed long type" in {
    val bytes = List( 6, 0xFE, 0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01).map(_.toByte).toArray

    val result: Long = YsonDecoder.decode(bytes, IndexedDataType.AtomicType(UInt64Type)).asInstanceOf[Long]

    UInt64Long(result) shouldEqual UInt64Long("18446744073709551358")
  }
}
