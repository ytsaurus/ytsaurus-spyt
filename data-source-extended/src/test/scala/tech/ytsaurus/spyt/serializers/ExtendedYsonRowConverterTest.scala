package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.Row
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.types.UInt64Long

class ExtendedYsonRowConverterTest extends AnyFlatSpec with Matchers {

  behavior of "YsonRowConverterTest"

  it should "convert UInt64 value" in {
    val converter = YsonRowConverter.getOrCreate(
      StructType(Seq(StructField("value", UInt64Type))),
      config = YsonEncoderConfig(skipNulls = false, typeV3Format = true)
    )

    val resultSmall = converter.serialize(Row(UInt64Long("12345")))
    resultSmall shouldEqual Seq(123, 1, 10, 118, 97, 108, 117, 101, 61, 6, 0xB9, 0x60, 125).map(_.toByte).toArray

    val resultBig = converter.serialize(Row(UInt64Long("18446744073709551358")))
    resultBig shouldEqual Seq(123, 1, 10, 118, 97, 108, 117, 101, 61, 6, 0xFE, 0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 125).map(_.toByte).toArray
  }

}
