package tech.ytsaurus.spyt.fs

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StringType}
import org.apache.spark.sql.spyt.types.YsonBinary
import org.apache.spark.test.UtilsWrapper
import org.mockito.scalatest.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, LocalYt, TestRow, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.ysontree.YTree
import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}

import scala.collection.mutable
import scala.language.postfixOps

class YtSparkSQLTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with MockitoSugar 
  with TableDrivenPropertyChecks with DynTableTestUtils with YtDistributedReadingTestUtils {
  import spark.implicits._

  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val anotherSchema = TableSchema.builder()
    .addValue("a", ColumnValueType.INT64)
    .addValue("d", ColumnValueType.STRING)
    .build()

  private val complexSchema = TableSchema.builder()
    .addValue("array", ColumnValueType.ANY)
    .addValue("map", ColumnValueType.ANY)
    .build()

  private val testModes = Table(
    "optimizeFor",
    OptimizeMode.Scan,
    OptimizeMode.Lookup
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sessionState.catalog.invalidateAllCachedTables()
  }

  testWithDistributedReading("select rows using views") { _ =>
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), path, atomicSchema, optimizeFor)

      val table = spark.read.yt(path)
      table.createOrReplaceTempView("table")
      val res = spark.sql(s"SELECT * FROM table")

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )
    }
  }

  testWithDistributedReading("select rows") { _ =>
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$path`")

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )

      val res2 = spark.sql(s"SELECT * FROM yt.`$path`")

      res2.columns should contain theSameElementsAs Seq("a", "b", "c")
      res2.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )
    }
  }

  testWithDistributedReading("select rows with wrong schema") { _ =>
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), tmpPath, atomicSchema)

    val res = spark.sql(s"SELECT * FROM yt.`yt:/$tmpPath`")
    res.collect() should contain theSameElementsAs Seq(Row(1, "a", 0.3))
  }

  testWithDistributedReading("select rows in complex table") { _ =>
    val data = Seq("""{array = [1; 2; 3]; map = {k1 = "a"; k2 = "b"}}""")
    val correctResult = Array(Seq(
      YsonEncoder.encode(List(1L, 2L, 3L), ArrayType(LongType), skipNulls = false).toList,
      YsonEncoder.encode(Map("k1" -> "a", "k2" -> "b"), MapType(StringType, StringType), skipNulls = false).toList
    ))

    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(data, path, complexSchema, optimizeFor)

      val df = spark.sql(s"SELECT * FROM yt.`ytTable:/$path`")
      df.columns should contain theSameElementsAs Seq("array", "map")

      val res = df.select("array", "map").collect()
        .map(row => row.toSeq.map(value => value.asInstanceOf[YsonBinary].bytes.toList))
      res should contain theSameElementsAs correctResult
    }
  }

  testWithDistributedReading("apply functions") { _ =>
    Seq(1, 2).toDF("a").write.yt(tmpPath)
    val res = spark.sql(s"SELECT md5(CAST (a as STRING)) FROM yt.`ytTable:/$tmpPath`")

    res.collect() should contain theSameElementsAs Seq(
      Row("c4ca4238a0b923820dcc509a6f75849b"),
      Row("c81e728d9d4c2f636f067f89cc14862c")
    )
  }

  testWithDistributedReading("filter rows") { _ =>
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}""",
        """{a = 3; b = "c"; c = 1.0}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$path` WHERE a > 1")
      res.collect() should contain theSameElementsAs Seq(
        Row(2, "b", 0.5),
        Row(3, "c", 1.0)
      )
    }
  }

  testWithDistributedReading("pushdown filters") { _ =>

    YtWrapper.createDir(tmpPath)
    withConfs(Map(s"spark.yt.${SparkSettings.Read.KeyColumnsFilterPushdown.Enabled.name}" -> "true",
      s"spark.yt.${SparkSettings.Read.YtPartitioningEnabled.name}" -> "true")
    ) {
      forAll(testModes) { optimizeFor =>
        val path = s"$tmpPath/${optimizeFor.name}"

        val data = (1L to 1000L).map(x => (x, x % 2))
        val df = data.toDF("a", "b").repartition(2)
        df.sort("a", "b").write.sortedBy("a", "b").yt(path)

        val table = spark.read.yt(path)
        table.createOrReplaceTempView("table")

        val resDf = spark.sql(s"SELECT * FROM table WHERE a >= 49 AND a <= 50 AND b == 1")
        val res = resDf.collect()
        val expectedData = data.filter { case (a, b) => a >= 49 && a <= 50 && b == 1 }

        scanOutputRows(resDf) should equal(2) // number of rows read from YT with pushdown filters
        res should contain theSameElementsAs expectedData.map(Row.fromTuple)
      }
    }
  }

  testWithDistributedReading("sort rows") { _ =>
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 3; b = "c"; c = 1.0}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$path` ORDER BY a DESC")
      res.collect() shouldBe Seq(
        Row(3, "c", 1.0),
        Row(2, "b", 0.5),
        Row(1, "a", 0.3)
      )
    }
  }

  testWithDistributedReading("group rows") { _ =>
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 1; b = "b"; c = 0.5}""",
        """{a = 2; b = "c"; c = 0.0}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT a, COUNT(*) FROM yt.`ytTable:/$path` GROUP BY a")
      res.collect() should contain theSameElementsAs Seq(
        Row(1, 2),
        Row(2, 1)
      )
    }
  }

  testWithDistributedReading("join tables") { _ =>
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path1 = s"$tmpPath/${optimizeFor.name}_1"
      writeTableFromYson(Seq(
        """{a = 2; b = "b"; c = 0.5}""",
        """{a = 2; b = "c"; c = 0.0}"""
      ), path1, atomicSchema, optimizeFor)

      val path2 = s"$tmpPath/${optimizeFor.name}_2"
      writeTableFromYson(Seq(
        """{a = 2; d = "hello"}""",
        """{a = 2; d = "ytsaurus"}""",
        """{a = 3; d = "spark"}"""
      ), path2, anotherSchema, optimizeFor)

      val res = spark.sql(
        s"""
           |SELECT t1.a, t2.d
           |FROM yt.`ytTable:/$path1` t1
           |INNER JOIN yt.`ytTable:/$path2` t2 ON t1.a == t2.a""".stripMargin
      )
      res.collect() should contain theSameElementsAs Seq(
        Row(2, "hello"), Row(2, "ytsaurus"),
        Row(2, "hello"), Row(2, "ytsaurus"),
      )
    }
  }

  testWithDistributedReading("select from dynamic table") { _ =>
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    // @latest_version is required
    // otherwise it will be appended to path in runtime and fail because of nested "directory" reading
    val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath/@timestamp_-1`")
    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  testWithDistributedReading("select from dynamic table without timestamp attribute") { _ =>
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`")
    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  testWithDistributedReading("select from a table using custom UDF") { _ =>
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}""",
      """{a = 3; b = "c"; c = 1.0}"""
    ), tmpPath, atomicSchema)

    val myUdf = (x: Int) => x * 10
    spark.udf.register("my_udf", myUdf)

    val df = spark.sql(s"SELECT my_udf(a), my_udf(10) FROM yt.`$tmpPath`")

    df.collect() should contain theSameElementsAs Seq(
      Row(10, 100), Row(20, 100), Row(30, 100)
    )
  }

  testWithDistributedReading("select from a table using constant expressions") { _ =>
    writeTableFromYson(Seq(
      """{a = 2; d = "hello"}"""
    ), tmpPath, anotherSchema)

    val df = spark.sql(s"SELECT 10 FROM yt.`$tmpPath`")

    df.collect() should contain theSameElementsAs Seq(Row(10))
  }

  testWithDistributedReading("join static table with dynamic one") { _ =>
    val path1 = s"$tmpPath/dynamic"
    prepareTestTable(path1, testData, Seq(Seq(), Seq(3), Seq(6, 12)))

    val path2 = s"$tmpPath/static"
    writeTableFromYson(Seq(
      """{a = 5; b = "13"; c = 0.0}""",
      """{a = 6; b = "11"; c = 0.0}""",
      """{a = 5; b = "10"; c = 0.0}"""
    ), path2, atomicSchema)

    val res = spark.sql(
      s"""
         |SELECT t1.a, t2.b
         |FROM yt.`ytTable:/$path1/@timestamp_-1` t1
         |INNER JOIN yt.`ytTable:/$path2` t2
         |ON t1.a == t2.a AND t1.b != CAST(t2.b AS INT)""".stripMargin
    )
    res.columns should contain theSameElementsAs Seq("a", "b")
    res.collect() should contain theSameElementsAs Seq(
      Row(5, "13"),
      Row(6, "11")
    )
  }

  testWithDistributedReading("cast nested null values") { _ =>
    val df = spark.sql("SELECT col1, col3, cast(array(NULL) as array<int>) FROM VALUES (1, 2, 3)")
    val result = df.collect()

    result should contain theSameElementsAs Seq(Row(1, 3, mutable.WrappedArray.make(Array(null))))
  }

  testWithDistributedReading("create table") { _ =>
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, name STRING, age INT) USING yt")
    val res = spark.read.yt(tmpPath)
    res.columns should contain theSameElementsAs Seq("id", "name", "age")
    res.collect() should contain theSameElementsAs Seq()

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, name STRING, age INT) USING yt")
    }
  }

  testWithDistributedReading("create a table without specifying ytTable:/ prefix") { _ =>
    spark.sql(s"CREATE TABLE yt.`$tmpPath` (id INT, name STRING, age INT) USING yt")
    val res = spark.read.yt(tmpPath)
    res.columns should contain theSameElementsAs Seq("id", "name", "age")
    res.collect() should contain theSameElementsAs Seq()
  }

  testWithDistributedReading("create table with custom attributes") { _ =>
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, name STRING, age INT) " +
      s"USING yt TBLPROPERTIES ('custom1'='value1','custom2'='4','key_columns'='[id]')")

    val attrs = YtWrapper.attributes(YPath.simple(tmpPath))
    attrs("custom1") shouldBe YTree.stringNode("value1")
    attrs("custom2") shouldBe YTree.integerNode(4)
    val schema = YtWrapper.attribute(tmpPath, "schema")
    schema.getAttribute("unique_keys").get() shouldBe YTree.booleanNode(false)
    attrs("sorted_by") shouldBe YTree.listBuilder().value(YTree.stringNode("id")).endList().build()
  }

  testWithDistributedReading("create table as select") { _ =>
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` USING yt AS SELECT col1, col2 FROM VALUES (1, false)")
    val res = spark.read.yt(tmpPath)
    res.columns should contain theSameElementsAs Seq("col1", "col2")
    res.collect() should contain theSameElementsAs Seq(Row(1, false))
  }

  testWithDistributedReading("create table as select from existing table") { _ =>
    YtWrapper.createDir(tmpPath)
    val originalPath = s"$tmpPath/original"
    val copyPath = s"$tmpPath/copy"
    writeTableFromYson(Seq(
      """{a = 1; d = "a"}""",
      """{a = 2; d = "b"}"""
    ), originalPath, anotherSchema)

    spark.sql(s"CREATE TABLE yt.`$copyPath` USING yt AS SELECT * FROM yt.`$originalPath`")
    val res = spark.read.yt(copyPath)
    res.columns should contain theSameElementsAs Seq("a", "d")
    res.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"))
  }

  testWithDistributedReading("drop table") { _ =>
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, age INT) USING yt")
    YtWrapper.exists(tmpPath) shouldBe true

    spark.sql(s"DROP TABLE yt.`ytTable:/$tmpPath`")
    YtWrapper.exists(tmpPath) shouldBe false

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"DROP TABLE yt.`ytTable:/$tmpPath`")
    }

    spark.sql(s"DROP TABLE IF EXISTS yt.`ytTable:/$tmpPath`")
  }

  testWithDistributedReading("not create a table or other cypress node when there were errors") { _ =>
    YtWrapper.createDir(tmpPath)
    val originalPath = s"$tmpPath/original"
    val copyPath = s"$tmpPath/copy"
    writeTableFromYson(Seq(
      """{a = 1; d = "a"}""",
      """{a = 2; d = "b"}"""
    ), originalPath, anotherSchema)

    val bogusUdf: Int => Int = (a: Int) => if (a == 2) a else throw new RuntimeException("Should be a bug here")
    spark.udf.register("bogus_udf", bogusUdf)

    a[SparkException] shouldBe thrownBy {
      spark.sql(s"CREATE TABLE yt.`$copyPath` USING yt AS SELECT bogus_udf(a) FROM yt.`$originalPath`")
    }

    YtWrapper.exists(copyPath) shouldBe false
  }

  testWithDistributedReading("insert to table") { _ =>
    YtWrapper.createDir(tmpPath)
    val path = s"$tmpPath/original"
    val path2 = s"$tmpPath/copy"
    writeTableFromYson(Seq(
      """{a = 1; d = "a"}""",
      """{a = 2; d = "b"}"""
    ), path, anotherSchema)

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"INSERT INTO TABLE yt.`ytTable:/$path2` SELECT * FROM yt.`ytTable:/$path`")
    }

    spark.sql(s"CREATE TABLE yt.`ytTable:/$path2` (a INT, d STRING) USING yt")
    spark.sql(s"INSERT INTO TABLE yt.`ytTable:/$path2` SELECT * FROM yt.`ytTable:/$path`")
    spark.sql(s"INSERT INTO TABLE yt.`ytTable:/$path2`(a, d) VALUES (3, 'c')")

    val res = spark.read.yt(path2)
    res.columns should contain theSameElementsAs Seq("a", "d")
    res.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))

    spark.sql(s"INSERT OVERWRITE TABLE yt.`ytTable:/$path2`(a, d) VALUES (4, 'd')")
    val res2 = spark.read.yt(path2)
    res2.collect() should contain theSameElementsAs Seq(Row(4, "d"))

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"INSERT OVERWRITE TABLE yt.`ytTable:/$path2`(c1) VALUES (0l)")
    }
  }

  testWithDistributedReading("work without specified scheme") { _ =>
    spark.sql(s"CREATE TABLE yt.`$tmpPath`(id INT) USING yt")
    spark.sql(s"INSERT INTO TABLE yt.`$tmpPath` VALUES (7), (6), (5)")
    val res = spark.sql(s"SELECT * FROM yt.`$tmpPath`")
    res.collect() should contain theSameElementsAs Seq(Row(7), Row(6), Row(5))
  }

  testWithDistributedReading("refresh when modified externally") { _ =>
    writeTableFromYson(Seq(
      """{a = 1; b = "qwe"; c = 0.0}""",
    ), tmpPath, atomicSchema)
    val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`")
    res.collect() shouldBe Seq(Row(1, "qwe", 0.0))

    YtWrapper.remove(tmpPath)
    writeTableFromYson(Seq(
      """{a = 1; d = "str1"}""",
      """{a = 2; d = "str2"}"""
    ), tmpPath, anotherSchema)
    spark.sql(s"REFRESH TABLE yt.`ytTable:/$tmpPath`")
    val res2 = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`")
    res2.collect() shouldBe Seq(Row(1, "str1"), Row(2, "str2"))
  }

  // TODO: wrap with testWithDistributedReading when TRspReadTablePartitionMeta will contain statistics
  it should "count io statistics" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Stream.from(1).take(1000)

    val store = UtilsWrapper.appStatusStore(spark)
    val stagesBefore = store.stageList(null)
    val totalInputBefore = stagesBefore.map(_.inputBytes).sum
    val totalOutputBefore = stagesBefore.map(_.outputBytes).sum

    data.toDF().coalesce(1).write.yt(customPath)
    val allRows = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`").collect()
    allRows should have size data.length

    val stages = store.stageList(null)
    val totalInput = stages.map(_.inputBytes).sum
    val totalOutput = stages.map(_.outputBytes).sum

    totalInput should be > totalInputBefore
    totalOutput should be > totalOutputBefore
  }

  testWithDistributedReading("work with cluster specification") { _ =>
    val df1 = Seq((1, "q"), (3, "c")).toDF("num", "name")
    df1.write.yt(tmpPath)

    val res = spark.sql(s"""SELECT * FROM yt.`<cluster="${LocalYt.proxy}">$tmpPath`""")
    res.collect() should contain theSameElementsAs Seq(Row(1, "q"), Row(3, "c"))

    val res2 = spark.sql(s"""SELECT * FROM yt.`ytTable:/<cluster="${LocalYt.proxy}">$tmpPath`""")
    res2.collect() should contain theSameElementsAs Seq(Row(1, "q"), Row(3, "c"))
  }

  testWithDistributedReading("cast some expression to string without any errors") { _ =>
    val df = spark.sql(s"""SELECT cast((date('2025-01-17') - INTERVAL 2 WEEK) AS STRING) AS some_date""")
    df.collect() should contain theSameElementsAs Seq(Row("2025-01-03"))
  }

  testWithDistributedReading("handle self-join with nullable columns using SQL") { _ =>
    YtWrapper.createDir(tmpPath)
    val sourcePath = s"$tmpPath/source"
    val cachePath = s"$tmpPath/cache"

    val nullableSchema = TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("a", TiType.optional(TiType.string()))
      .addValue("b", TiType.optional(TiType.string()))
      .addValue("c", TiType.optional(TiType.string()))
      .addValue("d", TiType.optional(TiType.string()))
      .build()

    writeTableFromYson(Seq(
      """{a = "val1"; b = "val2"; c = "val3"; d = "val4"}""",
      """{a = "val1"; b = "val2"; c = #; d = #}""",
      """{a = #; b = #; c = "val3"; d = "val4"}""",
      """{a = #; b = #; c = #; d = #}"""
    ), sourcePath, nullableSchema, OptimizeMode.Scan)

    withConfs(Map("spark.hadoop.yt.read.arrow.enabled" -> "true")) {
      spark.sql(
        s"""CREATE TABLE yt.`ytTable:/$cachePath` USING yt
           |AS (SELECT * FROM yt.`ytTable:/$sourcePath`)""".stripMargin)

      spark.sql(s"CREATE OR REPLACE TEMPORARY VIEW test_view AS SELECT * FROM yt.`ytTable:/$cachePath`")

      val res = spark.sql(
        """SELECT v1.a, v1.b
          |FROM test_view AS v1
          |JOIN test_view AS v2 ON v1.a = v2.a AND v1.b = v2.b""".stripMargin)
      res.collect().length should be >= 1
    }
  }


  it should "SQL API column pruning reduces input bytes for scan-optimized table with distributed reading" in {
    withConfs(distributedReadingEnabledConf(true)) {
      YtWrapper.createDir(tmpPath)
      val path = s"$tmpPath/wide_table"

      val wideSchema = TableSchema.builder()
        .setUniqueKeys(false)
        .addValue("a", TiType.optional(TiType.string()))
        .addValue("b", TiType.optional(TiType.string()))
        .addValue("c", TiType.optional(TiType.string()))
        .addValue("d", TiType.optional(TiType.string()))
        .addValue("e", TiType.optional(TiType.string()))
        .build()

      val longStr = "x" * 200
      val rows = (1 to 500).map(i =>
        s"""{a = "$longStr$i"; b = "$longStr$i"; c = "$longStr$i"; d = "$longStr$i"; e = "$longStr$i"}"""
      )
      writeTableFromYson(rows, path, wideSchema, OptimizeMode.Scan)

      val store = UtilsWrapper.appStatusStore(spark)

      val inputBefore1 = store.stageList(null).map(_.inputBytes).sum
      spark.sql(s"SELECT * FROM yt.`ytTable:/$path`").collect()
      val bytesAll = store.stageList(null).map(_.inputBytes).sum - inputBefore1

      val inputBefore2 = store.stageList(null).map(_.inputBytes).sum
      spark.sql(s"SELECT a FROM yt.`ytTable:/$path`").collect()
      val bytesPruned = store.stageList(null).map(_.inputBytes).sum - inputBefore2

      bytesAll should be > 0L
      bytesPruned should be > 0L
      bytesPruned should be < bytesAll
    }
  }
}
