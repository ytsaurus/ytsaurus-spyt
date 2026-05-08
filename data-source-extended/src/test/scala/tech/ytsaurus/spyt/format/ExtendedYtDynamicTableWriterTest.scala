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

import scala.concurrent.duration._
import scala.language.postfixOps


class ExtendedYtDynamicTableWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers{
  it should "write composite types to dynamic table" in {
    val tableSchema = TableSchema.builder()
      .addKey("key", ColumnValueType.UINT64)
      .addValue("list", TiType.list(TiType.int64))
      .addValue("dict", TiType.dict(TiType.string(), TiType.doubleType()))
      .addValue("struct", TiType.struct(
        new Member("field1", TiType.int64()),
        new Member("field2", TiType.int64()),
        new Member("field3", TiType.string())))
      .addValue("yson", TiType.optional(TiType.yson()))
      .build()

    YtWrapper.createDynTable(tmpPath, tableSchema)
    YtWrapper.mountTableSync(tmpPath, 1.minute)

    val schema = new StructType()
      .add("key", UInt64Type)
      .add("list", ArrayType(LongType))
      .add("dict", MapType(StringType, DoubleType))
      .add("struct", StructType(Seq(
        StructField("field1", LongType),
        StructField("field2", LongType),
        StructField("field3", StringType))))
      .add("yson", YsonType)

    val data = Seq(
      Row(UInt64Long(1L), Seq(1L, 2L, 3L), Map("1" -> 0.1, "2" -> 0.2), Row(1L, 1L, "str"),
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
        .key("key").value(1L)
        .key("list").value(YTree.listBuilder().value(1).value(2L).value(3L).buildList())
        .key("dict").value(YTree.listBuilder()
          .value(YTree.listBuilder().value("1").value(0.1).buildList())
          .value(YTree.listBuilder().value("2").value(0.2).buildList())
          .buildList())
        .key("struct").value(YTree.listBuilder().value(1L).value(1L).value("str").buildList())
        .key("yson").value(YTree.listBuilder().value(1).value(2L).value(3L).buildList())
        .buildMap()
    )

    val resultRows: Seq[YTreeMapNode] = YtWrapper.selectRows(tmpPath)

    resultRows should contain theSameElementsAs expectedData
  }
}