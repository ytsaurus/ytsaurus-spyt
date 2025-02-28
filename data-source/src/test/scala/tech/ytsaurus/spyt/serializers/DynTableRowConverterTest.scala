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

import scala.concurrent.duration._
import scala.language.postfixOps


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
    YtWrapper.mountTableSync(tmpPath, 1.minute)

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
}
