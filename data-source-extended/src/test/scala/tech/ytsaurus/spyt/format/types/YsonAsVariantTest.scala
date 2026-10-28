package tech.ytsaurus.spyt.format.types

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.FakeSortShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.spyt.types.YsonType
import org.apache.spark.sql.SaveMode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.client.request.{ReadSerializationContext, ReadTable}
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.whenVariantSupported
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{InconsistentDynamicWrite, WriteTypeV3}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.{OptimizeMode, SyncTableIterator}
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.yson.YsonBinaryWriter
import tech.ytsaurus.ysontree.{YTreeMapNode, YTreeNode, YTreeTextSerializer}

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.time.Duration
import scala.jdk.CollectionConverters._

class YsonAsVariantTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with YtDistributedReadingTestUtils with YsonAsVariantTestUtils {
  behavior of "YtDataSource with yson columns as Variant"

  private val anyCells: Seq[(ColumnValueType, Any)] = Seq(
    ColumnValueType.INT64 -> -1L,
    ColumnValueType.UINT64 -> 5L,
    ColumnValueType.UINT64 -> Long.MinValue,
    ColumnValueType.UINT64 -> -1L,
    ColumnValueType.DOUBLE -> 2.5,
    ColumnValueType.BOOLEAN -> true,
    ColumnValueType.STRING -> "été".getBytes(StandardCharsets.UTF_8),
    ColumnValueType.STRING -> Array(0xff, 0xfe).map(_.toByte),
    ColumnValueType.NULL -> null,
    ColumnValueType.ANY -> ysonBytes("""{b=1;a=[1;"c"]}"""),
    ColumnValueType.ANY -> ysonBytes("[<x=1>2;#]"))

  private val anyCellsYson = Seq("-1", "5u", "9223372036854775808u", "18446744073709551615u", "2.5", "%true",
    "\"été\"", "\"\\xFF\\xFE\"", null, """{a=[1;"c"];b=1}""", "[<x=1>2;#]")

  private val anyCellsJson = Seq("-1", "5", "9223372036854775808", "18446744073709551615", "2.5", "true", "\"été\"",
    "\"//4=\"", null, """{"a":[1,"c"],"b":1}""", """[{"$attributes":{"x":1},"$value":2},null]""")

  private val sortedByAnySchema = TableSchema.builder().setUniqueKeys(false)
    .add(new ColumnSchema("n", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("k", ColumnValueType.ANY, ColumnSortOrder.ASCENDING))
    .build()

  private val sortedByAnyKeyCount = 200L

  private val keyOptimizationsConf = Map(
    s"spark.yt.${Read.KeyPartitioning.Enabled.name}" -> "true",
    s"spark.yt.${Read.KeyPartitioning.UnionLimit.name}" -> "2",
    s"spark.yt.${Read.PlanOptimizationEnabled.name}" -> "true",
    s"spark.yt.${Read.KeyColumnsFilterPushdown.Enabled.name}" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb")

  private val variantWriteQuery =
    """SELECT id, parse_json(json) AS v, try_parse_json(text) AS nv FROM VALUES
      |  (1L, '{"b":[1,2.5,"s",null],"a":true}', '[1]'),
      |  (2L, '18446744073709551615', 'not json'),
      |  (3L, 'null', '"x"'),
      |  (4L, '-5', '2.5') AS t(id, json, text)
      |UNION ALL SELECT 5L, cast(date'2024-01-01' AS variant), NULL
      |UNION ALL SELECT 6L, cast(timestamp'2024-01-01 00:00:00.123456' AS variant), NULL
      |UNION ALL SELECT 7L, cast(cast(-12.50 AS decimal(10, 2)) AS variant), NULL
      |UNION ALL SELECT 8L, cast(X'0102FF' AS variant), NULL""".stripMargin

  private val variantWriteYson = Map(
    "v" -> Seq("{a=%true;b=[1;2.5;\"s\";#]}", "18446744073709551615u", "#", "-5", "\"2024-01-01\"",
      "\"2024-01-01 00:00:00.123456+00:00\"", "-12.5", "\"\\x01\\x02\\xFF\""),
    "nv" -> Seq("[1]", "#", "\"x\"", "2.5", "#", "#", "#", "#"))

  private val duplicateKeyYson: Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new YsonBinaryWriter(output)
    writer.onBeginMap()
    Seq(1L, 2L).foreach { value =>
      writer.onKeyedItem("a")
      writer.onInteger(value)
    }
    writer.onEndMap()
    writer.close()
    output.toByteArray
  }

  private def writeTwoYsonColumns(): Unit = {
    val schema = TableSchema.builder().setUniqueKeys(false)
      .addValue("id", ColumnValueType.INT64)
      .addValue("hinted", ColumnValueType.ANY)
      .addValue("other", ColumnValueType.ANY)
      .build()
    val rows = Seq("""{id = 1; hinted = {a = 1}; other = [2]}""", """{id = 2; hinted = [3]; other = {b = 4}}""")
    writeTableFromYson(rows, tmpPath, schema)
  }

  private def readYsonRows(path: String): Seq[YTreeMapNode] = {
    val request = new ReadTable[YTreeNode](YPath.simple(path), ReadSerializationContext.ysonBinary())
    new SyncTableIterator(yt.readTable(request).join(), Duration.ofMinutes(1)).toList.map(_.mapNode())
  }

  private def ysonTextsById(rows: Seq[YTreeMapNode], column: String): Map[Long, String] = {
    rows.map { row =>
      val yson = Option(row.asMap().get(column)).map(YTreeTextSerializer.stableSerialize).getOrElse("#")
      row.getOrThrow("id").longValue() -> yson
    }.toMap
  }

  private def ytColumnTypes(path: String): Map[String, TiType] = {
    val schema = TableSchema.fromYTree(YtWrapper.attribute(path, "schema"))
    schema.getColumns.asScala.map(column => column.getName -> column.getTypeV3).toMap
  }

  private def createMountedDynTable(path: String, schema: TableSchema): Unit = {
    YtWrapper.createDynTable(path, schema)
    YtWrapper.mountTableSync(path)
  }

  private def rootCause(error: Throwable): Throwable = {
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toSeq.last
  }

  private def sortedByAnyCells(n: Long): Seq[(ColumnValueType, Any, String, String)] = {
    val scalar: (ColumnValueType, Any, String, String) = n % 4 match {
      case 0L => (ColumnValueType.INT64, n, s"$n", s"$n")
      case 1L => (ColumnValueType.UINT64, n, s"$n", s"${n}u")
      case 2L => (ColumnValueType.DOUBLE, n + 0.5, s"$n.5", s"$n.5")
      case _ => (ColumnValueType.STRING, s"s$n".getBytes(StandardCharsets.UTF_8), "\"s" + n + "\"", "\"s" + n + "\"")
    }
    Seq(scalar, (ColumnValueType.ANY, ysonBytes(s"[$n;x]"), "[" + n + ",\"x\"]", s"[$n;x]"))
  }

  testWithDistributedReading("read a yson column as its schema hint says, with and without ysonAsVariant") { _ =>
    whenVariantSupported {
      val variantType = SparkAdapter.instance.ysonVariantConverter.variantType
      writeTwoYsonColumns()

      val df = spark.read.enableArrow.schemaHint("hinted" -> variantType).yt(tmpPath)

      df.schema("hinted").dataType shouldBe variantType
      df.schema("other").dataType shouldBe usualYsonType
      readsArrowBatches(df.select("id", "hinted")) shouldBe false
      columnTexts(df, "id", "hinted", ysonAsVariant = true).toMap shouldBe byId(Seq("""{"a":1}""", "[3]"))

      withConf(Read.YsonAsVariant, true) {
        val ysonTypeHinted = spark.read.enableArrow.schemaHint("hinted" -> new YsonType()).yt(tmpPath)
        ysonTypeHinted.schema("hinted").dataType shouldBe new YsonType()
        ysonTypeHinted.schema("other").dataType shouldBe variantType
        readsArrowBatches(ysonTypeHinted.select("id", "hinted")) shouldBe true
      }
    }
  }

  testWithYsonAsVariant("read a table sorted by an any column with key-based optimizations") { ysonAsVariant =>
    val cells = (1L to sortedByAnyKeyCount).flatMap(n => sortedByAnyCells(n).map(n -> _))
    val rows = cells.map { case (n, (valueType, value, _, _)) =>
      unversionedRow(ColumnValueType.INT64 -> n, valueType -> value)
    }
    writeTableFromURow(rows, tmpPath, sortedByAnySchema)
    val expected = cells.map { case (n, (_, _, json, yson)) => n -> (if (ysonAsVariant) json else canonicalYson(yson)) }

    withConfs(keyOptimizationsConf) {
      val df = spark.read.yt(tmpPath)
      val counts = df.groupBy("n").agg(count("k"))

      df.schema("k").dataType shouldBe ysonColumnType(ysonAsVariant)
      columnTexts(df, "n", "k", ysonAsVariant) should contain theSameElementsAs expected
      columnTexts(df.where("n >= 150"), "n", "k", ysonAsVariant) should contain theSameElementsAs
        expected.filter(_._1 >= 150)
      counts.collect().map(row => row.getLong(0) -> row.getLong(1)) should contain theSameElementsAs
        (1L to sortedByAnyKeyCount).map(_ -> 2L)
      executedPlanNodes(counts) { case fakeShuffle: FakeSortShuffleExchangeExec => fakeShuffle } should have size 1
      executedPlanNodes(counts) { case shuffle: ShuffleExchangeExec => shuffle } shouldBe empty
    }
  }

  testWithDistributedReading("apply spark.sql.variant.allowDuplicateKeys to yson columns read as Variant") { _ =>
    whenVariantSupported {
      val rows = anyColumnRows(Seq(ColumnValueType.ANY -> duplicateKeyYson))
      writeTableFromURow(rows, tmpPath, anyColumnSchema, OptimizeMode.Lookup)

      withConf(Read.YsonAsVariant, true) {
        val error = the[SparkException] thrownBy spark.read.yt(tmpPath).collect()
        rootCause(error).getMessage should include("VARIANT_DUPLICATE_KEY")
        withConf("spark.sql.variant.allowDuplicateKeys", "true") {
          variantJson(spark.read.yt(tmpPath), "value") shouldBe Seq("""{"a":2}""")
        }
      }
    }
  }

  Seq(true, false).foreach { writeTypeV3 =>
    it should s"write yson columns read as Variant back as YSON, writeTypeV3 = $writeTypeV3" in {
      whenVariantSupported {
        YtWrapper.createDir(tmpPath)
        val sourcePath = s"$tmpPath/source"
        val copyPath = s"$tmpPath/copy"
        writeTableFromURow(anyColumnRows(anyCells), sourcePath, anyColumnSchema, OptimizeMode.Lookup)
        val expectedYson = anyCellsYson.updated(1, "5").map(text => canonicalYson(Option(text).getOrElse("#")))

        withConf(Read.YsonAsVariant, true) {
          spark.read.yt(sourcePath).write.option(WriteTypeV3.name, writeTypeV3).yt(copyPath)

          ytColumnTypes(copyPath)("value") shouldBe TiType.optional(TiType.yson())
          ysonTextsById(readYsonRows(copyPath), "value") shouldBe byId(expectedYson)
          textsById(spark.read.yt(copyPath), ysonAsVariant = true) shouldBe byId(anyCellsJson)
        }
      }
    }
  }

  it should "write Variant values as YSON to a sorted dynamic table" in {
    whenVariantSupported {
      val schema = TableSchema.builder()
        .addKey("id", ColumnValueType.INT64)
        .addValue("v", TiType.optional(TiType.yson()))
        .addValue("nv", TiType.optional(TiType.yson()))
        .build()
      createMountedDynTable(tmpPath, schema)

      spark.sql(variantWriteQuery).write.mode(SaveMode.Append).option(InconsistentDynamicWrite.name, value = true)
        .yt(tmpPath)

      val rows = YtWrapper.selectRows(tmpPath)
      variantWriteYson.foreach { case (column, yson) =>
        ysonTextsById(rows, column) shouldBe byId(yson.map(canonicalYson))
      }
    }
  }

  it should "reject a Variant nested in a struct on a static table write" in {
    whenVariantSupported {
      val df = spark.sql("SELECT 1L AS id, named_struct('v', parse_json('1')) AS nested")

      the[IllegalArgumentException] thrownBy df.write.yt(tmpPath) should have message
        "YT data source does not support struct<v:variant> data type(column `nested`)."
      YtWrapper.exists(tmpPath) shouldBe false
    }
  }
}
