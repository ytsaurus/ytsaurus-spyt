package tech.ytsaurus.spyt.format

import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.serializers.SchemaConverter.SortOrder.{Asc, Desc}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.YtReadSettings
import tech.ytsaurus.ysontree.YTree

import scala.collection.JavaConverters._

class YtSortedTablesTest extends FlatSpec with Matchers with LocalSpark with TestUtils with TmpDir {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    yt.setNode("//sys/@config/enable_descending_sort_order", YTree.booleanNode(true)).join()
  }

  override def afterAll(): Unit = {
    yt.setNode("//sys/@config/enable_descending_sort_order", YTree.booleanNode(false)).join()
    super.afterAll()
  }

  it should "write sorted table" in {
    val df = (1 to 9).toDF.coalesce(3)

    df.write.sortedBy("value").yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("value")
    uniqueKeys(tmpPath) shouldEqual false
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> Some("value"), "type" -> Some("int32"), "sort_order" -> Some("ascending"))
    )
  }

  it should "read desc sorted table" in {
    val schema = TableSchema.builder()
      .setUniqueKeys(false)
      .add(new ColumnSchema("value", ColumnValueType.INT64,ColumnSortOrder.DESCENDING))
      .build()

    writeTableFromYson(Seq(
      """{value = 3}""",
      """{value = 2}""",
      """{value = 1}"""
    ), tmpPath, schema)

    val df = spark.read.yt(tmpPath)
    val values = df.select("value").as[Long].collect()
    values should contain theSameElementsInOrderAs Seq(3, 2, 1)
  }

  it should "write desc sorted table" in {
    val sampleData = (9 to 1 by -1)

    val df = spark.createDataset(sampleData).coalesce(3)
    df.write.sortedBy("value").sortOrders(Desc).yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("value")
    uniqueKeys(tmpPath) shouldEqual false
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> "value", "type" -> "int32", "sort_order" -> "descending").mapValues(Some(_))
    )
    val writtenData = readTableAsYson(tmpPath, readSettings = YtReadSettings.default.copy(unordered = false))
    val actual = writtenData.map(_.asMap().get("value").intValue())
    actual should contain theSameElementsInOrderAs sampleData
  }

  it should "write desc sorted table using option" in {
    val df = (9 to 1 by -1).toDF.coalesce(3)

    df.write.sortedBy("value").option("sort_orders", """["desc"]""").yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("value")
    uniqueKeys(tmpPath) shouldEqual false
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> "value", "type" -> "int32", "sort_order" -> "descending").mapValues(Some(_))
    )
  }

  it should "throw exception for incorrect sort_orders" in {
    val df = (9 to 1 by -1).toDF.coalesce(3)

    an[IllegalArgumentException] shouldBe thrownBy {
      df.write.sortedBy("value").option("sort_orders", """["descending"]""").yt(tmpPath)
    }

    an[IllegalArgumentException] shouldBe thrownBy {
      df.write.sortedBy("value").option("sort_orders", """["desc", "asc"]""").yt(tmpPath)
    }

    an[IllegalArgumentException] shouldBe thrownBy {
      df.write.sortedBy("value").sortOrders(Desc, Asc).yt(tmpPath)
    }
  }

  it should "read table with mixed sort orders" in {
    val schema = TableSchema.builder()
      .setUniqueKeys(false)
      .add(new ColumnSchema("key", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
      .add(new ColumnSchema("timestamp", ColumnValueType.INT64, ColumnSortOrder.DESCENDING))
      .add(new ColumnSchema("value", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
      .build()

    writeTableFromYson(Seq(
      """{key = 1; timestamp = 100; value = 10}""",
      """{key = 1; timestamp = 50; value = 20}""",
      """{key = 2; timestamp = 200; value = 5}"""
    ), tmpPath, schema)

    val result = spark.read.yt(tmpPath)
      .select("key", "timestamp", "value")
      .as[(Long, Long, Long)]
      .collect()

    result should contain theSameElementsInOrderAs Array((1L, 100L, 10L), (1L, 50L, 20L), (2L, 200L, 5L))
  }

  it should "write table with mixed sort orders" in {
    val data = Seq((1, 10, 100), (2, 20, 200), (3, 30, 300))
    val df = data.toDF("key", "timestamp", "value")
      .orderBy($"key".asc, $"timestamp".desc, $"value".asc)

    df.write
      .sortedBy("key", "timestamp", "value")
      .sortOrders(Asc, Desc, Asc)
      .yt(tmpPath)

    sortColumns(tmpPath) shouldBe Seq("key", "timestamp", "value")
    uniqueKeys(tmpPath) shouldBe false
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> "key", "type" -> "int32", "sort_order" -> "ascending").mapValues(Some(_)),
      Map("name" -> "timestamp", "type" -> "int32", "sort_order" -> "descending").mapValues(Some(_)),
      Map("name" -> "value", "type" -> "int32", "sort_order" -> "ascending").mapValues(Some(_))
    )
    val writtenData = readTableAsYson(tmpPath, readSettings = YtReadSettings.default.copy(unordered = false))
    val actual = writtenData.map(node => (node.asMap().get("key").longValue(),
      node.asMap().get("timestamp").longValue(),
      node.asMap().get("value").longValue()))
    actual should contain theSameElementsInOrderAs data

  }

  it should "change columns order in sorted table" in {
    val df = (1 to 9).zip(9 to 1 by -1).toDF("a", "b")

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    df.sort("b").coalesce(3).write.sortedBy("b").yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("b")
    uniqueKeys(tmpPath) shouldEqual false
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> Some("b"), "type" -> Some("int32"), "sort_order" -> Some("ascending")),
      Map("name" -> Some("a"), "type" -> Some("int32"), "sort_order" -> None)
    )
  }

  it should "abort transaction if failed to create sorted table" in {
    val df = (1 to 9).toDF("my_name").coalesce(3)
    an[Exception] shouldBe thrownBy {
      df.write.sortedBy("bad_name").yt(tmpPath)
    }
    noException shouldBe thrownBy {
      df.write.sortedBy("my_name").yt(tmpPath)
    }
  }

  it should "write table with 'unique_keys' attribute" in {
    val df = Seq(1, 2, 3).toDF()

    df.write.sortedByUniqueKeys("value").yt(tmpPath)

    sortColumns(tmpPath) should contain theSameElementsAs Seq("value")
    uniqueKeys(tmpPath) shouldEqual true
    ytSchema(tmpPath) should contain theSameElementsAs Seq(
      Map("name" -> Some("value"), "type" -> Some("int32"), "sort_order" -> Some("ascending"))
    )
  }

  it should "fail if 'unique_keys' attribute is set true, but keys are not unique" in {
    val df = Seq(1, 1, 1).toDF()

    an[Exception] shouldBe thrownBy {
      df.write.sortedByUniqueKeys("value").yt(tmpPath)
    }
  }

  it should "fail if 'unique_keys' attribute is set true, but table is not sorted" in {
    val df = Seq(1, 2, 3).toDF()

    an[Exception] shouldBe thrownBy {
      df.write.uniqueKeys.yt(tmpPath)
    }
  }

  it should "fail if 'sorted_by' option is set, but table is not sorted" in {
    val df = Seq(2, 3, 1).toDF().coalesce(1)

    an[Exception] shouldBe thrownBy {
      df.write.sortedBy("value").yt(tmpPath)
    }
  }

  def sortColumns(path: String): Seq[String] = {
    YtWrapper.attribute(path, "sorted_by").asList().asScala.map(_.stringValue())
  }

  def uniqueKeys(path: String): Boolean = {
    YtWrapper.attribute(path, "schema").getAttribute("unique_keys").get().boolValue()
  }

  def ytSchema(path: String): Seq[Map[String, Option[String]]] = {
    import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
    val schemaFields = Seq("name", "type", "sort_order")
    YtWrapper.attribute(path, "schema").asList().asScala.map { field =>
      val map = field.asMap()
      schemaFields.map(n => n -> map.getOption(n).map(_.stringValue())).toMap
    }
  }

}
