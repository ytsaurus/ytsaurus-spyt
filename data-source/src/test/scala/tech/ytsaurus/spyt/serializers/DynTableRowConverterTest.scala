package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtWriter
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.InconsistentDynamicWrite
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeMapNode}

import java.time.Duration

class DynTableRowConverterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils
  with DynTableTestUtils {

  it should "convert typeV3 data recursively" in {
    val tableSchema = TableSchema.builder()
      .addKey("key", ColumnValueType.INT64)
      .addValue("list", TiType.list(TiType.struct(
        new Member("dict", TiType.dict(
          TiType.string(),
          TiType.dict(TiType.int32(), TiType.doubleType())
        ))
      )))
      .build()

    YtWrapper.createDynTable(tmpPath, tableSchema)
    YtWrapper.mountTableSync(tmpPath, Duration.ofMinutes(1))

    val schema = new StructType()
      .add("key", LongType)
      .add("list", ArrayType(StructType(Seq(
        StructField("dict", MapType(StringType, MapType(IntegerType, DoubleType)))
      ))))

    val data = Seq(
      Row(1L, Seq(Row(Map("1" -> Map(1 -> 0.1, 2 -> 0.2)))))
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
        .key("key").value(1L)
        .key("list").value(
          YTree.listBuilder().value(
              YTree.listBuilder().value(
                  YTree.listBuilder()
                    .value(
                      YTree.listBuilder()
                        .value("1")
                        .value(YTree.listBuilder()
                          .value(YTree.listBuilder().value(1).value(0.1).buildList())
                          .value(YTree.listBuilder().value(2).value(0.2).buildList())
                          .buildList())
                        .buildList())
                    .buildList())
                .buildList())
            .buildList())
        .buildMap()
    )

    val resultRows: Seq[YTreeMapNode] = YtWrapper.selectRows(tmpPath)

    resultRows should contain theSameElementsAs expectedData
  }

  it should "convert list, struct and map with uint8/uint16/uint32/uint64 preserving unsigned semantics" in {
    val tableSchema = TableSchema.builder()
      .addKey("key", ColumnValueType.INT64)
      .addValue("uints", TiType.list(TiType.uint8()))
      .addValue("info", TiType.struct(
        new Member("id", TiType.uint64()),
        new Member("count", TiType.uint32()),
        new Member("name", TiType.string())
      ))
      .addValue("counts", TiType.dict(TiType.string(), TiType.uint16()))
      .build()

    YtWrapper.createDynTable(tmpPath, tableSchema)
    YtWrapper.mountTableSync(tmpPath, Duration.ofMinutes(1))

    val schema = new StructType()
      .add("key", LongType)
      .add("uints", ArrayType(LongType))
      .add("info", StructType(Seq(
        StructField("id", LongType),
        StructField("count", LongType),
        StructField("name", StringType)
      )))
      .add("counts", MapType(StringType, LongType))

    val data = Seq(
      Row(
        1L,
        Seq(0L, 1L, 255L),
        Row(java.lang.Long.MAX_VALUE, 4294967295L, "abc"),
        Map("answer" -> 65535L)
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
        .key("key").value(1L)
        .key("uints").value(
          YTree.listBuilder()
            .unsignedValue(0L)
            .unsignedValue(1L)
            .unsignedValue(255L)
            .buildList())
        .key("info").value(
          YTree.listBuilder()
            .unsignedValue(java.lang.Long.MAX_VALUE)
            .unsignedValue(4294967295L)
            .value("abc")
            .buildList())
        .key("counts").value(
          YTree.listBuilder()
            .value(YTree.listBuilder()
              .value("answer")
              .unsignedValue(65535L)
              .buildList())
            .buildList())
        .buildMap()
    )

    val resultRows: Seq[YTreeMapNode] = YtWrapper.selectRows(tmpPath)

    resultRows should contain theSameElementsAs expectedData
  }

  it should "map row values by column names when spark and yt schema orders differ" in {
    val sparkSchema = StructType(Seq(
      StructField("a", LongType),
      StructField("b", LongType),
      StructField("c", StringType),
      StructField("d", LongType)
    ))

    val tableSchema = TableSchema.builder()
      .addKey("c", ColumnValueType.STRING)
      .addKey("a", ColumnValueType.INT64)
      .addKey("b", ColumnValueType.INT64)
      .addValue("d", ColumnValueType.INT64)
      .build()

    val converter = new DynTableRowConverter(sparkSchema, tableSchema, typeV3 = false)
    val row = Seq[Any](2L, 5L, "queue-1", 42L)

    converter.convertRow(row) shouldEqual Seq[Any]("queue-1", 2L, 5L, 42L)
  }

  it should "throw IllegalArgumentException when yt schema contains key column missing in spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("known", LongType),
      StructField("another", LongType)
    ))

    val tableSchema = TableSchema.builder()
      .addKey("missing_in_spark_schema", ColumnValueType.INT64)
      .addValue("known", ColumnValueType.INT64)
      .addValue("another", ColumnValueType.INT64)
      .build()

    val error = the[IllegalArgumentException] thrownBy new DynTableRowConverter(sparkSchema, tableSchema, typeV3 = false)
    error.getMessage should include("Cannot find Spark field for YT key column 'missing_in_spark_schema'")
  }

  it should "write null for non-key yt columns missing in spark schema" in {
    val sparkSchema = StructType(Seq(
      StructField("known", LongType),
      StructField("another", LongType)
    ))

    val tableSchema = TableSchema.builder()
      .addKey("known", ColumnValueType.INT64)
      .addValue("missing_optional_value", ColumnValueType.STRING)
      .addValue("another", ColumnValueType.INT64)
      .build()

    val converter = new DynTableRowConverter(sparkSchema, tableSchema, typeV3 = false)

    converter.convertRow(Seq[Any](1L, 2L)) shouldEqual Seq[Any](1L, null, 2L)
  }

  it should "throw IllegalArgumentException when spark schema contains columns absent in yt schema" in {
    val sparkSchema = StructType(Seq(
      StructField("known", LongType),
      StructField("extra_in_spark", LongType)
    ))

    val tableSchema = TableSchema.builder()
      .addKey("known", ColumnValueType.INT64)
      .build()

    val error = the[IllegalArgumentException] thrownBy new DynTableRowConverter(sparkSchema, tableSchema, typeV3 = false)
    error.getMessage should include("Cannot find YT columns for Spark fields: [extra_in_spark]")
  }
}
