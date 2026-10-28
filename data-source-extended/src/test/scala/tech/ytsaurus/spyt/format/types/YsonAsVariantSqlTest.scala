package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.yt.YtSourceScanExec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.whenVariantSupported
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.typeinfo.TiType

class YsonAsVariantSqlTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with YsonAsVariantTestUtils {
  behavior of "yt.`path` with yson columns as Variant"

  override def reinstantiateSparkSession: Boolean = true

  private val ysonAsVariantKey = s"spark.yt.${Read.YsonAsVariant.name}"

  private val anyCells: Seq[(ColumnValueType, Any)] = Seq(
    ColumnValueType.UINT64 -> -1L,
    ColumnValueType.ANY -> ysonBytes("""{b=[1;"c"];a=%true}"""),
    ColumnValueType.NULL -> null)

  private val anyCellsJson = Seq("18446744073709551615", """{"a":true,"b":[1,"c"]}""", null)

  it should "read yson columns of a scan table with ysonAsVariant in the startup settings" in {
    whenVariantSupported {
      val variantType = SparkAdapter.instance.ysonVariantConverter.variantType
      writeTableFromURow(anyColumnRows(anyCells), tmpPath, anyColumnSchema, OptimizeMode.Scan)

      withSparkSession(Map(ysonAsVariantKey -> "true")) { session =>
        val df = session.sql(s"SELECT * FROM yt.`$tmpPath`")

        df.schema("value").dataType shouldBe variantType
        textsById(df, ysonAsVariant = true) shouldBe byId(anyCellsJson)
        executedPlanNodes(df) { case scan: YtSourceScanExec => scan.supportsColumnar } shouldBe Seq(false)
        executedPlanNodes(df) { case columnarToRow: ColumnarToRowExec => columnarToRow } shouldBe empty
        val ids = session.sql(s"SELECT id FROM yt.`$tmpPath`")
        executedPlanNodes(ids) { case scan: YtSourceScanExec => scan.supportsColumnar } shouldBe Seq(true)
      }
    }
  }

  it should "ignore ysonAsVariant set at runtime in yt.`path`, unlike spark.read.yt" in {
    whenVariantSupported {
      val variantType = SparkAdapter.instance.ysonVariantConverter.variantType
      writeTableFromURow(anyColumnRows(anyCells), tmpPath, anyColumnSchema)

      withConf(Read.YsonAsVariant, true) {
        spark.sql(s"SELECT * FROM yt.`$tmpPath`").schema("value").dataType shouldBe usualYsonType
        spark.read.yt(tmpPath).schema("value").dataType shouldBe variantType
      }
    }
  }

  it should "create a table with a Variant column and read it back through yt.`path`" in {
    whenVariantSupported {
      withSparkSession(Map(ysonAsVariantKey -> "true")) { session =>
        session.sql(s"CREATE TABLE yt.`$tmpPath` (id BIGINT, value VARIANT) USING yt")
        session.sql(
          s"""INSERT INTO TABLE yt.`$tmpPath` VALUES (1, parse_json('{"a":18446744073709551615}')), (2, NULL)""")

        val ytSchema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
        ytSchema.getColumnSchema(ytSchema.findColumn("value")).getTypeV3 shouldBe TiType.optional(TiType.yson())
        textsById(session.sql(s"SELECT * FROM yt.`$tmpPath`"), ysonAsVariant = true) shouldBe
          byId(Seq("""{"a":18446744073709551615}""", null))
      }
    }
  }
}
