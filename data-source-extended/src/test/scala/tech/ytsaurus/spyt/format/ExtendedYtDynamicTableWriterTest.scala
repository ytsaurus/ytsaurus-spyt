package tech.ytsaurus.spyt.format

import org.apache.spark.sql.spyt.types.{UInt64Type, YsonBinary, YsonType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtWriter
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.InconsistentDynamicWrite
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.types.UInt64Long
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeMapNode}

import java.time.Duration


class ExtendedYtDynamicTableWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers{
  it should "write composite types with UInt64Type and mixed numeric types to dynamic table" in {
    val bigValue = UInt64Long("9223372036854775816")

    val tableSchema = TableSchema.builder()
      .addKey("key", ColumnValueType.UINT64)
      .addValue("list_uint64", TiType.list(TiType.uint64()))
      .addValue("list_long", TiType.list(TiType.int64()))
      .addValue("list_int", TiType.list(TiType.int32()))
      .addValue("list_double", TiType.list(TiType.doubleType()))
      .addValue("dict_uint64", TiType.dict(TiType.string(), TiType.uint64()))
      .addValue("dict_utf8", TiType.dict(TiType.string(), TiType.utf8()))
      .addValue("dict_float", TiType.dict(TiType.string(), TiType.floatType()))
      .addValue("struct_uint64", TiType.struct(
        new Member("field1", TiType.uint64()),
        new Member("field2", TiType.uint64()),
        new Member("field3", TiType.string())))
      .addValue("struct_int_float_bool", TiType.struct(
        new Member("field1", TiType.int32()),
        new Member("field2", TiType.floatType()),
        new Member("field3", TiType.bool())))
      .addValue("yson", TiType.optional(TiType.yson()))
      .build()

    YtWrapper.createDynTable(tmpPath, tableSchema)
    YtWrapper.mountTableSync(tmpPath, Duration.ofMinutes(1))

    val schema = new StructType()
      .add("key", UInt64Type)
      .add("list_uint64", ArrayType(UInt64Type))
      .add("list_long", ArrayType(LongType))
      .add("list_int", ArrayType(IntegerType))
      .add("list_double", ArrayType(DoubleType))
      .add("dict_uint64", MapType(StringType, UInt64Type))
      .add("dict_utf8", MapType(StringType, StringType))
      .add("dict_float", MapType(StringType, FloatType))
      .add("struct_uint64", StructType(Seq(
        StructField("field1", UInt64Type),
        StructField("field2", UInt64Type),
        StructField("field3", StringType))))
      .add("struct_int_float_bool", StructType(Seq(
        StructField("field1", IntegerType),
        StructField("field2", FloatType),
        StructField("field3", BooleanType))))
      .add("yson", YsonType)

    val data = Seq(
      Row(UInt64Long(1L),
        Seq(UInt64Long(1L), bigValue, UInt64Long(3L)),
        Seq(1L, 2L, 3L),
        Seq(10, 20, 30),
        Seq(0.25d, 1.5d, 2.75d),
        Map("1" -> bigValue, "2" -> UInt64Long(2L)),
        Map("1" -> "a", "2" -> "b"),
        Map("1" -> 11.5f, "2" -> 22.75f),
        Row(bigValue, UInt64Long(1L), "str"),
        Row(42, 3.25f, true),
        YsonBinary(YsonEncoder.encode(List(1L, 2L, 3L), ArrayType(LongType), skipNulls = false))
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df
      .write
      .mode(SaveMode.Append)
      .option(YtTableSparkSettings.WriteTypeV3.name, value = true)
      .option(InconsistentDynamicWrite, true)
      .yt(tmpPath)

    val expectedData = Seq(
      YTree.mapBuilder()
        .key("key").unsignedValue(1L)
        .key("list_uint64").value(
          YTree.listBuilder()
            .unsignedValue(1L)
            .unsignedValue(bigValue.toLong)
            .unsignedValue(3L)
            .buildList())
        .key("list_long").value(
          YTree.listBuilder()
            .value(1L)
            .value(2L)
            .value(3L)
            .buildList())
        .key("list_int").value(
          YTree.listBuilder()
            .value(10)
            .value(20)
            .value(30)
            .buildList())
        .key("list_double").value(
          YTree.listBuilder()
            .value(0.25d)
            .value(1.5d)
            .value(2.75d)
            .buildList())
        .key("dict_uint64").value(YTree.listBuilder()
          .value(YTree.listBuilder().value("1").unsignedValue(bigValue.toLong).buildList())
          .value(YTree.listBuilder().value("2").unsignedValue(2L).buildList())
          .buildList())
        .key("dict_utf8").value(YTree.listBuilder()
          .value(YTree.listBuilder().value("1").value("a").buildList())
          .value(YTree.listBuilder().value("2").value("b").buildList())
          .buildList())
        .key("dict_float").value(YTree.listBuilder()
          .value(YTree.listBuilder().value("1").value(11.5d).buildList())
          .value(YTree.listBuilder().value("2").value(22.75d).buildList())
          .buildList())
        .key("struct_uint64").value(
          YTree.listBuilder()
            .unsignedValue(bigValue.toLong)
            .unsignedValue(1L)
            .value("str")
            .buildList())
        .key("struct_int_float_bool").value(
          YTree.listBuilder()
            .value(42)
            .value(3.25d)
            .value(true)
            .buildList())
        .key("yson").value(YTree.listBuilder().value(1).value(2L).value(3L).buildList())
        .buildMap()
    )

    val resultRows: Seq[YTreeMapNode] = YtWrapper.selectRows(tmpPath)

    resultRows should contain theSameElementsAs expectedData
  }
}
