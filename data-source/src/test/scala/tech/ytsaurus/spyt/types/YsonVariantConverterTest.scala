package tech.ytsaurus.spyt.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.{SparkAdapter, YsonAsVariantTestUtils}
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.whenVariantSupported
import tech.ytsaurus.spyt.test.LocalSpark
import tech.ytsaurus.yson.YsonBinaryWriter

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.{Collections, UUID}

class YsonVariantConverterTest extends AnyFlatSpec with Matchers with LocalSpark with YsonAsVariantTestUtils {

  behavior of "YsonVariantConverter"

  private val nonUtf8Bytes = Array(0xff, 0xfe).map(_.toByte)

  private def converter: YsonVariantConverter = SparkAdapter.instance.ysonVariantConverter

  private def fromYson(bytes: Array[Byte], allowDuplicateKeys: Boolean): Any = {
    YsonVariantCodec.fromYson(converter, bytes, allowDuplicateKeys)
  }

  private def toYson(variant: Any): Array[Byte] = YsonVariantCodec.toYson(converter, variant)

  private def yson(text: String): Array[Byte] = text.getBytes(StandardCharsets.UTF_8)

  private def binaryYson(write: YsonBinaryWriter => Unit): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new YsonBinaryWriter(output)
    write(writer)
    writer.close()
    output.toByteArray
  }

  private def jsonOf(bytes: Array[Byte], allowDuplicateKeys: Boolean = false): String = {
    fromYson(bytes, allowDuplicateKeys).toString
  }

  private def variantSchemaOf(variant: Any): String = {
    val schema = StructType(Seq(StructField("v", converter.variantType)))
    spark.createDataFrame(Collections.singletonList(Row(variant)), schema)
      .selectExpr("schema_of_variant(v)").collect().head.getString(0)
  }

  private def ysonOfSql(expression: String): Array[Byte] = {
    toYson(spark.sql(s"SELECT $expression").collect().head.get(0))
  }

  it should "convert a non-UTF-8 string to a binary" in {
    whenVariantSupported {
      jsonOf(binaryYson(_.onString(nonUtf8Bytes, 0, nonUtf8Bytes.length))) shouldBe "\"//4=\""
    }
  }

  it should "fail on a YSON key that is not valid UTF-8" in {
    whenVariantSupported {
      val nonUtf8KeyYson = binaryYson { writer =>
        writer.onBeginMap()
        writer.onKeyedItem(nonUtf8Bytes, 0, nonUtf8Bytes.length)
        writer.onInteger(1)
        writer.onEndMap()
      }
      val error = the[IllegalArgumentException] thrownBy fromYson(nonUtf8KeyYson, allowDuplicateKeys = true)
      error.getMessage should include("YSON key \"\\xFF\\xFE\" is not valid UTF-8")
    }
  }

  it should "keep valid UTF-8 keys and strings that contain U+FFFD" in {
    whenVariantSupported {
      jsonOf(yson("{\"a\uFFFD\"=\"b\uFFFD\"}")) shouldBe "{\"a\uFFFD\":\"b\uFFFD\"}"
    }
  }

  it should "escape keys that start with $ the way the YTsaurus JSON format does" in {
    whenVariantSupported {
      val inputYson = """{k={"$attributes"={a=1};"$value"=5};"$"=<"$x"=1>2}"""
      val variant = fromYson(yson(inputYson), allowDuplicateKeys = false)
      variant.toString shouldBe
        """{"$$":{"$attributes":{"$$x":1},"$value":2},"k":{"$$attributes":{"a":1},"$$value":5}}"""
      ysonText(toYson(variant)) shouldBe canonicalYson(inputYson)
    }
  }

  it should "return null for a top-level entity without attributes" in {
    whenVariantSupported {
      Option(fromYson(yson("#"), allowDuplicateKeys = false)) shouldBe None
      jsonOf(yson("<a=1>#")) shouldBe """{"$attributes":{"a":1},"$value":null}"""
    }
  }

  it should "fail on a repeated map key unless duplicate keys are allowed" in {
    whenVariantSupported {
      val duplicateKeyYson = yson("{a=1;a=2}")
      val error = the[RuntimeException] thrownBy fromYson(duplicateKeyYson, allowDuplicateKeys = false)
      error.getMessage should include("VARIANT_DUPLICATE_KEY")
      jsonOf(duplicateKeyYson, allowDuplicateKeys = true) shouldBe """{"a":2}"""
    }
  }

  Seq[(String, YsonVariantConverter => Any, String, String)](
    ("an int64", _.fromInt64(-42), "BIGINT", "-42"),
    ("a uint64 up to Long.MaxValue", _.fromUint64(Long.MaxValue), "BIGINT", "9223372036854775807"),
    ("the uint64 2^63", _.fromUint64(Long.MinValue), "DECIMAL(19,0)", "9223372036854775808"),
    ("an empty string", _.fromString(Array.emptyByteArray), "STRING", "\"\"")
  ).foreach { case (description, convert, expectedSchema, expectedJson) =>
    it should s"convert $description from an unpacked scalar" in {
      whenVariantSupported {
        val variant = convert(converter)
        variantSchemaOf(variant) shouldBe expectedSchema
        variant.toString shouldBe expectedJson
      }
    }
  }

  it should "build the same Variant from an unpacked scalar as from the same YSON scalar" in {
    whenVariantSupported {
      converter.fromInt64(-5) shouldBe fromYson(yson("-5"), allowDuplicateKeys = false)
      converter.fromUint64(100) shouldBe fromYson(yson("100u"), allowDuplicateKeys = false)
      converter.fromUint64(-1L) shouldBe fromYson(yson("18446744073709551615u"), allowDuplicateKeys = false)
      converter.fromDouble(Double.NaN) shouldBe fromYson(yson("%nan"), allowDuplicateKeys = false)
      converter.fromDouble(Double.NegativeInfinity) shouldBe fromYson(yson("%-inf"), allowDuplicateKeys = false)
      converter.fromBoolean(false) shouldBe fromYson(yson("%false"), allowDuplicateKeys = false)
      converter.fromString(nonUtf8Bytes) shouldBe
        fromYson(binaryYson(_.onString(nonUtf8Bytes, 0, nonUtf8Bytes.length)), allowDuplicateKeys = false)
    }
  }

  Seq(
    ("a Variant null", "parse_json('null')", "#"),
    ("a decimal integer within int64", "cast(cast(-12 AS decimal(10, 0)) AS variant)", "-12"),
    ("the decimal 2^63", "parse_json('9223372036854775808')", "9223372036854775808u"),
    ("a decimal above uint64", "parse_json('18446744073709551616')", "18446744073709551616.0"),
    ("a decimal below int64", "parse_json('-9223372036854775809')", "-9223372036854775809.0"),
    ("a float", "cast(cast(1.25 AS float) AS variant)", "1.25"),
    ("a string", "parse_json('\"été\"')", "\"été\""),
    ("a date", "cast(date'2024-01-01' AS variant)", "\"2024-01-01\""),
    ("a timestamp without time zone", "cast(timestamp_ntz'2024-01-01 00:00:00.123456' AS variant)",
      "\"2024-01-01 00:00:00.123456\""),
    ("decimals nested in objects and arrays", """parse_json('{"a":{"b":[[1.25,5.0]]},"c":18446744073709551615}')""",
      "{a={b=[[1.25;5.0]]};c=18446744073709551615u}"),
    ("keys with $ and objects that only look like a node with attributes",
      """parse_json('{"$ref":1,"$$x":2,"k":{"$attributes":1,"$value":5},"m":{"$attributes":{},"$value":5,"x":1}}')""",
      """{"$x"=2;"$ref"=1;k={"$attributes"=1;"$value"=5};m={"$attributes"={};"$value"=5;x=1}}""")
  ).foreach { case (description, expression, expectedYson) =>
    it should s"write $description as YSON" in {
      whenVariantSupported {
        ysonText(ysonOfSql(expression)) shouldBe canonicalYson(expectedYson)
      }
    }
  }

  Seq(
    ("an object of $attributes and $value at the top level", """parse_json('{"$attributes":{"a":1},"$value":5}')""",
      "cannot be a top-level value"),
    ("an object of $attributes and $value as the $value of another one",
      """parse_json('{"k":{"$attributes":{"a":1},"$value":{"$attributes":{"b":2},"$value":5}}}')""",
      "the value of another node with attributes"),
    ("an object with both $x and $$x", """parse_json('{"$x":1,"$$x":2}')""", "both become the YSON key \"$x\"")
  ).foreach { case (description, expression, expectedError) =>
    it should s"fail to write $description as YSON" in {
      whenVariantSupported {
        val error = the[IllegalArgumentException] thrownBy ysonOfSql(expression)
        error.getMessage should include(expectedError)
      }
    }
  }

  it should "write a timestamp in UTC whatever the session time zone is" in {
    whenVariantSupported {
      withConf("spark.sql.session.timeZone", "America/Los_Angeles") {
        ysonText(ysonOfSql("cast(timestamp'2024-01-01 00:00:00.123456' AS variant)")) shouldBe
          canonicalYson("\"2024-01-01 08:00:00.123456+00:00\"")
      }
    }
  }

  it should "write a UUID as its text" in {
    whenVariantSupported {
      val uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000")
      // Spark SQL cannot make a Variant UUID, so its bytes are built by hand: the primitive header of type 20
      // followed by the 16 big-endian bytes of the UUID, and the metadata of version 1 with no keys
      val value = ByteBuffer.allocate(17).put((20 << 2).toByte)
        .putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()
      val metadata = Array[Byte](1, 0, 0)
      // VariantVal is built by reflection because this test also compiles against Spark 3
      val variant = Class.forName("org.apache.spark.unsafe.types.VariantVal")
        .getConstructor(classOf[Array[Byte]], classOf[Array[Byte]]).newInstance(value, metadata)
      val quotedUuid = "\"" + uuid + "\""
      variant.toString shouldBe quotedUuid
      ysonText(toYson(variant)) shouldBe canonicalYson(quotedUuid)
    }
  }

  Seq(
    ("integers of both signs", "{a=1;b=[2u;18446744073709551615u;-3];c=-9223372036854775808}",
      "{a=1;b=[2;18446744073709551615u;-3];c=-9223372036854775808}"),
    ("other scalars", "[%true;%false;#;2.5;5.0;%nan;%-inf;\"s\"]", "[%true;%false;#;2.5;5.0;%nan;%-inf;\"s\"]"),
    ("nested and empty containers", "{a=[[1];[]];b={}}", "{a=[[1];[]];b={}}"),
    ("nodes with attributes", "{k=<x=y;n=<m=1>2>{z=1};l=[<a=#>1]}", "{k=<x=y;n=<m=1>2>{z=1};l=[<a=#>1]}"),
    ("keys that start with $ and $$", """{"$x"=1;"$$x"=2}""", """{"$x"=1;"$$x"=2}""")
  ).foreach { case (description, inputYson, expectedYson) =>
    it should s"keep the values of $description through a round trip from YSON" in {
      whenVariantSupported {
        val variant = fromYson(yson(inputYson), allowDuplicateKeys = false)
        ysonText(toYson(variant)) shouldBe canonicalYson(expectedYson)
      }
    }
  }
}
