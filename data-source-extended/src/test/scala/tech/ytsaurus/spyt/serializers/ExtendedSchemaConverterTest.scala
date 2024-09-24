package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.spyt.types.{DatetimeType, UInt64Type, YsonType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.{SchemaTestUtils, YtReader}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.TypeV3
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeMapNode}


class ExtendedSchemaConverterTest extends AnyFlatSpec with Matchers
  with TestUtils with TmpDir with LocalSpark with SchemaTestUtils {

  import SchemaConverterTest._

  private val extendedSparkSchema = sparkSchema
    .add(structField("UInt64", UInt64Type, nullable = false))
    .add(structField("Yson", YsonType, nullable = false))
    .add(structField("Struct_with_yson", StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", YsonType, nullable = false))), nullable = false))

  it should "read schema without parsing type v3" in {
    // in sparkSchema.toYTree no type_v1 type names
    spark.conf.set(s"spark.yt.${TypeV3.name}", value = false)
    createEmptyTable(tmpPath, schema)
    val res = spark.read.yt(tmpPath).schema

    spark.conf.set(s"spark.yt.${TypeV3.name}", value = true)
    val res2 = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, "false").yt(tmpPath).schema

    res shouldBe res2
    res shouldBe StructType(Seq(
      structField("NULL", NullType, nullable = true),
      structField("INT64", LongType, nullable = true),
      structField("int64_3", LongType, nullable = true),
      structField("UINT64", UInt64Type, nullable = true),
      structField("uint64_3", UInt64Type, nullable = true),
      structField("floatType", FloatType, nullable = true, arrowSupported = false),
      structField("DOUBLE", DoubleType, nullable = true),
      structField("doubleType", DoubleType, nullable = true),
      structField("BOOLEAN", BooleanType, nullable = true),
      structField("bool", BooleanType, nullable = true),
      structField("STRING", StringType, nullable = true),
      structField("string_3", StringType, nullable = true),
      structField("ANY", YsonType, nullable = true),
      structField("yson", YsonType, nullable = true),
      structField("int8", ByteType, nullable = true),
      structField("uint8", ShortType, nullable = true),
      structField("int16", ShortType, nullable = true),
      structField("uint16", IntegerType, nullable = true),
      structField("int32", IntegerType, nullable = true),
      structField("uint32", LongType, nullable = true),
      structField("decimal", StringType, nullable = true),
      structField("utf8", StringType, nullable = true),
      structField("date", DateType, nullable = true, arrowSupported = false),
      structField("datetime", new DatetimeType(), nullable = true, arrowSupported = false),
      structField("timestamp", TimestampType, nullable = true, arrowSupported = false),
      structField("interval", LongType, nullable = true, arrowSupported = false),
      structField("list", YsonType, nullable = true),
      structField("dict", YsonType, nullable = true),
      structField("struct", YsonType, nullable = true),
      structField("tuple", YsonType, nullable = true),
      structField("variantOverStruct", YsonType, nullable = true),
      structField("variantOverTuple", YsonType, nullable = true)
    ))
  }

  it should "convert supported types with parsing type v3" in {
    val res = SchemaConverter.sparkSchema(schema.toYTree, parsingTypeV3 = true)
    res shouldBe StructType(Seq(
      structField("NULL", NullType, nullable = false),
      structField("INT64", LongType, nullable = true),
      structField("int64_3", LongType, nullable = false),
      structField("UINT64", UInt64Type, nullable = true),
      structField("uint64_3", UInt64Type, nullable = false),
      structField("floatType", FloatType, nullable = false, arrowSupported = false),
      structField("DOUBLE", DoubleType, nullable = true),
      structField("doubleType", DoubleType, nullable = false),
      structField("BOOLEAN", BooleanType, nullable = true),
      structField("bool", BooleanType, nullable = false),
      structField("STRING", StringType, nullable = true),
      structField("string_3", StringType, nullable = false),
      structField("ANY", YsonType, nullable = true),
      structField("yson", YsonType, nullable = true),
      structField("int8", ByteType, nullable = false),
      structField("uint8", ShortType, nullable = false),
      structField("int16", ShortType, nullable = false),
      structField("uint16", IntegerType, nullable = false),
      structField("int32", IntegerType, nullable = false),
      structField("uint32", LongType, nullable = false),
      structField("decimal", DecimalType(22, 0), nullable = false),
      structField("utf8", StringType, nullable = false),
      structField("date", DateType, nullable = false, arrowSupported = false),
      structField("datetime", new DatetimeType(), nullable = false, arrowSupported = false),
      structField("timestamp", TimestampType, nullable = false, arrowSupported = false),
      structField("interval", LongType, nullable = false, arrowSupported = false),
      structField("list", ArrayType(BooleanType, containsNull = false), nullable = false),
      structField("dict", MapType(DoubleType, StringType, valueContainsNull = false), nullable = false),
      structField("struct", StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", YsonType, nullable = true))), nullable = false),
      structField("tuple", StructType(Seq(StructField("_1", BooleanType, nullable = false), StructField("_2", DateType, nullable = false))), nullable = false),
      structField("variantOverStruct", StructType(Seq(StructField("_vc", IntegerType, metadata = new MetadataBuilder().putBoolean("optional", false).build()),
        StructField("_vd", TimestampType, metadata = new MetadataBuilder().putBoolean("optional", false).build()))), nullable = false),
      structField("variantOverTuple", StructType(Seq(StructField("_v_1", FloatType, metadata = new MetadataBuilder().putBoolean("optional", false).build()),
        StructField("_v_2", LongType, metadata = new MetadataBuilder().putBoolean("optional", false).build()))), nullable = false)
    ))
  }

  it should "use schema hint" in {
    val schema = TableSchema.builder()
      .setUniqueKeys(false)
      .addKey("a", ColumnValueType.STRING)
      .addValue("b", ColumnValueType.INT64)
      .build()
    val res = SchemaConverter.sparkSchema(schema.toYTree,
      Some(StructType(Seq(structField("a", UInt64Type, Some("x"), 1, nullable = false))))
    )
    res shouldBe StructType(Seq(
      structField("a", UInt64Type, keyId = 0, nullable = false),
      structField("b", LongType, nullable = true)
    ))
  }

  it should "convert spark schema to yt one with parsing type v3" in {
    val res = TableSchema.fromYTree(WriteSchemaConverter(Map.empty, typeV3Format = true).ytLogicalSchema(extendedSparkSchema, Unordered))
    res shouldBe TableSchema.builder().setUniqueKeys(false)
      .addValue("Null", TiType.nullType())
      .addValue("Long", TiType.int64())
      .addValue("Float", TiType.floatType())
      .addValue("Double", TiType.doubleType())
      .addValue("Boolean", TiType.bool())
      .addValue("String", TiType.string())
      .addValue("Binary", TiType.string())
      .addValue("Byte", TiType.int8())
      .addValue("Short", TiType.int16())
      .addValue("Integer", TiType.int32())
      .addValue("Decimal", TiType.decimal(22,0))
      .addValue("Date", TiType.date())
      .addValue("Datetime", TiType.datetime())
      .addValue("Timestamp", TiType.timestamp())
      .addValue("Array", TiType.list(TiType.bool()))
      .addValue("Map", TiType.dict(TiType.doubleType(), TiType.string()))
      .addValue("Struct", TiType.struct(new Member("a", TiType.string()), new Member("b", TiType.string())))
      .addValue("Tuple", TiType.tuple(TiType.bool(), TiType.date()))
      .addValue("VariantOverStruct",
        TiType.variantOverStruct(java.util.List.of[Member](new Member("c", TiType.int32()), new Member("d", TiType.int64()))))
      .addValue("VariantOverTuple", TiType.variantOverTuple(TiType.floatType(), TiType.int64()))
      .addValue("UInt64", TiType.uint64())
      .addValue("Yson", TiType.yson())
      .addValue("Struct_with_yson", TiType.struct(new Member("a", TiType.string()), new Member("b", TiType.yson())))
      .build()
  }

  it should "convert spark schema to yt one" in {
    import scala.collection.JavaConverters._
    def getColumn(name: String, t: String): YTreeMapNode = {
      YTree.builder.beginMap.key("name").value(name).key("type").value(t)
        .key("required").value(false).buildMap
    }
    val res = new WriteSchemaConverter(Map.empty, typeV3Format = false).ytLogicalSchema(extendedSparkSchema, Unordered)
    res shouldBe YTree.builder
      .beginAttributes
      .key("strict").value(true)
      .key("unique_keys").value(false)
      .endAttributes
      .value(Seq(
        getColumn("Null", "null"),
        getColumn("Long", "int64"),
        getColumn("Float", "float"),
        getColumn("Double", "double"),
        getColumn("Boolean", "boolean"),
        getColumn("String", "string"),
        getColumn("Binary", "string"),
        getColumn("Byte", "int8"),
        getColumn("Short", "int16"),
        getColumn("Integer", "int32"),
        getColumn("Decimal", "any"),
        getColumn("Date", "date"),
        getColumn("Datetime", "datetime"),
        getColumn("Timestamp", "timestamp"),
        getColumn("Array", "any"),
        getColumn("Map", "any"),
        getColumn("Struct", "any"),
        getColumn("Tuple", "any"),
        getColumn("VariantOverStruct", "any"),
        getColumn("VariantOverTuple", "any"),
        getColumn("UInt64", "uint64"),
        getColumn("Yson", "any"),
        getColumn("Struct_with_yson", "any"),
      ).asJava)
      .build
  }
}
