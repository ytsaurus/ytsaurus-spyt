package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.rows.WireValueDeserializer
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.whenVariantSupported
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.YsonAsVariant
import tech.ytsaurus.spyt.types.{YsonVariantCodec, YsonVariantConverter}
import tech.ytsaurus.spyt.wrapper.config.sparkConfigKeys
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.yson.YsonBinaryWriter

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

class WireDeserializerTest extends AnyFlatSpec with Matchers {

  behavior of "WireDeserializer"

  private val ysonColumnSchema = TableSchema.builder().addValue("v", TiType.optional(TiType.yson())).build()

  private val nonUtf8Bytes = Array(0xff, 0xfe).map(_.toByte)

  private val duplicateKeyYson = yson("{a=1;a=2}")

  private def converter: YsonVariantConverter = SparkAdapter.instance.ysonVariantConverter

  private def variantSchema: StructType = StructType(Seq(StructField("v", converter.variantType)))

  private def yson(text: String): Array[Byte] = text.getBytes(StandardCharsets.UTF_8)

  private def binaryYsonString(bytes: Array[Byte]): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new YsonBinaryWriter(output)
    writer.onString(bytes, 0, bytes.length)
    writer.close()
    output.toByteArray
  }

  private def withDuplicateKeysAllowed[T](allowed: Boolean)(body: => T): T = {
    val conf = new SQLConf()
    conf.setConfString("spark.sql.variant.allowDuplicateKeys", allowed.toString)
    SQLConf.withExistingConf(conf)(body)
  }

  private def readRow[T](
    deserializer: WireDeserializer[T],
    valueType: ColumnValueType,
    feed: WireValueDeserializer[_] => Unit): T = {
    deserializer.updateSchema(ysonColumnSchema)
    val valueDeserializer = deserializer.onNewRow(1)
    valueDeserializer.setId(0)
    valueDeserializer.setType(valueType)
    feed(valueDeserializer)
    deserializer.onCompleteRow()
  }

  private def firstValue(row: Any): Any = row match {
    case internalRow: InternalRow => if (internalRow.isNullAt(0)) null else internalRow.get(0, converter.variantType)
    case values: Array[_] => values.head
    case other => fail(s"Unexpected deserialized row $other")
  }

  private def readVariant(valueType: ColumnValueType, feed: WireValueDeserializer[_] => Unit): Any = {
    firstValue(readRow(new InternalRowDeserializer(variantSchema), valueType, feed))
  }

  it should "handle NULL values for columns not present in Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType),
      StructField("b", StringType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("b", TiType.string())
      .addValue("extra1", TiType.int64())
      .addValue("extra2", TiType.string())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(4)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(42L)

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.STRING)
    valueDeserializer.onBytes("hello".getBytes("UTF-8"))

    valueDeserializer.setId(2)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    valueDeserializer.setId(3)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    val row: InternalRow = deserializer.onCompleteRow()

    row.getLong(0) shouldEqual 42L
    row.getString(1) shouldEqual "hello"
  }

  it should "handle NULL values for columns present in Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType),
      StructField("b", StringType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("b", TiType.string())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(2)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.STRING)
    valueDeserializer.onBytes("test".getBytes("UTF-8"))

    val row: InternalRow = deserializer.onCompleteRow()

    row.isNullAt(0) shouldEqual true
    row.getString(1) shouldEqual "test"
  }

  it should "handle server returning columns in different order" in {
    val sparkSchema = StructType(Seq(
      StructField("b", StringType),
      StructField("a", LongType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("b", TiType.string())
      .addValue("extra", TiType.int64())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(3)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(100L)

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.STRING)
    valueDeserializer.onBytes("world".getBytes("UTF-8"))

    valueDeserializer.setId(2)
    valueDeserializer.setType(ColumnValueType.NULL)
    valueDeserializer.onEntity()

    val row: InternalRow = deserializer.onCompleteRow()

    row.getString(0) shouldEqual "world"
    row.getLong(1) shouldEqual 100L
  }

  it should "handle non-NULL values for columns not present in Spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType)
    ))

    val serverSchema = TableSchema.builder()
      .addValue("a", TiType.int64())
      .addValue("extra", TiType.int64())
      .build()

    val deserializer = new InternalRowDeserializer(sparkSchema)
    deserializer.updateSchema(serverSchema)

    val valueDeserializer = deserializer.onNewRow(2)

    valueDeserializer.setId(0)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(42L)

    valueDeserializer.setId(1)
    valueDeserializer.setType(ColumnValueType.INT64)
    valueDeserializer.onInteger(999L)

    val row: InternalRow = deserializer.onCompleteRow()

    row.getLong(0) shouldEqual 42L
  }

  Seq[(String, ColumnValueType, WireValueDeserializer[_] => Unit, Array[Byte])](
    ("an int64", ColumnValueType.INT64, _.onInteger(-42), yson("-42")),
    ("the largest uint64", ColumnValueType.UINT64, _.onInteger(-1L), yson("18446744073709551615u")),
    ("a double", ColumnValueType.DOUBLE, _.onDouble(2.5), yson("2.5")),
    ("a boolean", ColumnValueType.BOOLEAN, _.onBoolean(true), yson("%true")),
    ("a non-UTF-8 string", ColumnValueType.STRING, _.onBytes(nonUtf8Bytes), binaryYsonString(nonUtf8Bytes))
  ).foreach { case (description, valueType, feed, sameValueYson) =>
    it should s"read $description of a Variant column as the Variant of the same YSON value" in {
      whenVariantSupported {
        readVariant(valueType, feed) shouldBe
          YsonVariantCodec.fromYson(converter, sameValueYson, allowDuplicateKeys = false)
      }
    }
  }

  it should "read a map of a Variant column from its YSON" in {
    whenVariantSupported {
      readVariant(ColumnValueType.ANY, _.onBytes(yson("{b=1;a=[2u;%true]}"))).toString shouldBe
        """{"a":[2,true],"b":1}"""
    }
  }

  it should "name the column and the YSON read options when a Variant key is not valid UTF-8" in {
    whenVariantSupported {
      val output = new ByteArrayOutputStream()
      val writer = new YsonBinaryWriter(output)
      writer.onBeginMap()
      writer.onKeyedItem(nonUtf8Bytes, 0, nonUtf8Bytes.length)
      writer.onInteger(1)
      writer.onEndMap()
      writer.close()

      val error = the[IllegalArgumentException] thrownBy readVariant(ColumnValueType.ANY, _.onBytes(output.toByteArray))

      error.getMessage should include("Column v cannot be read as Variant: YSON key \"\\xFF\\xFE\" is not valid UTF-8")
      error.getMessage should include(sparkConfigKeys(YsonAsVariant))
    }
  }

  it should "decode Variant values only for the columns of the Spark Variant type" in {
    whenVariantSupported {
      val sparkSchema = StructType(Seq(
        StructField("v", converter.variantType),
        StructField("a", LongType),
        StructField("b", BinaryType)))

      val serverSchema = TableSchema.builder()
        .addValue("a", TiType.int64())
        .addValue("b", TiType.optional(TiType.yson()))
        .addValue("v", TiType.optional(TiType.yson()))
        .build()

      val deserializer = new InternalRowDeserializer(sparkSchema)
      deserializer.updateSchema(serverSchema)

      val valueDeserializer = deserializer.onNewRow(3)

      valueDeserializer.setId(0)
      valueDeserializer.setType(ColumnValueType.INT64)
      valueDeserializer.onInteger(7L)

      valueDeserializer.setId(1)
      valueDeserializer.setType(ColumnValueType.ANY)
      valueDeserializer.onBytes(yson("{x=1}"))

      valueDeserializer.setId(2)
      valueDeserializer.setType(ColumnValueType.UINT64)
      valueDeserializer.onInteger(-1L)

      val row: InternalRow = deserializer.onCompleteRow()

      row.get(0, converter.variantType).toString shouldEqual "18446744073709551615"
      row.getLong(1) shouldEqual 7L
      row.getBinary(2) shouldEqual yson("{x=1}")
    }
  }

  Seq[(String, StructType => WireDeserializer[_])](
    ("InternalRowDeserializer", InternalRowDeserializer.getOrCreate(_)),
    ("ArrayAnyDeserializer", ArrayAnyDeserializer.getOrCreate(_))
  ).foreach { case (deserializerName, getOrCreate) =>
    it should s"cache $deserializerName instances per duplicate key policy of the current SQL conf" in {
      whenVariantSupported {
        val allowing = withDuplicateKeysAllowed(allowed = true)(getOrCreate(variantSchema))
        val rejecting = withDuplicateKeysAllowed(allowed = false)(getOrCreate(variantSchema))
        rejecting should not be theSameInstanceAs(allowing)
        withDuplicateKeysAllowed(allowed = true)(getOrCreate(variantSchema)) should be theSameInstanceAs allowing

        firstValue(readRow(allowing, ColumnValueType.ANY, _.onBytes(duplicateKeyYson))).toString shouldBe """{"a":2}"""
        a[RuntimeException] should be thrownBy readRow(rejecting, ColumnValueType.ANY, _.onBytes(duplicateKeyYson))
      }
    }
  }

  it should "ignore the duplicate key setting for schemas without Variant columns" in {
    val schema = StructType(Seq(StructField("a", BinaryType), StructField("b", StringType)))
    withDuplicateKeysAllowed(allowed = true)(WireDeserializer.allowDuplicateVariantKeys(schema)) shouldBe false
  }
}
