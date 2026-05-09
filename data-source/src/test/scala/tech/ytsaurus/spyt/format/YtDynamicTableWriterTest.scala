package tech.ytsaurus.spyt.format

import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.exceptions._
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.InconsistentDynamicWrite
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered
import tech.ytsaurus.spyt.serializers.WriteSchemaConverter
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.{YtReadContext, YtReadSettings}
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeMapNode}

import java.time.Duration

class YtDynamicTableWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers {

  import YtDynamicTableWriterTest._

  override val numExecutors = 8

  "YtDynamicTableWriter" should "write lots of data to yt" in {
    doTheTest(dataSize = 1000000)
  }

  it should "check if the inconsistent_dynamic_write is explicitly set to true" in {
    an[InconsistentDynamicWriteException] should be thrownBy {
      doTheTest(setInconsistentDynamicWrite = false)
    }
  }

  it should "check that the batch size is less than or equal 50K" in {
    a[TooLargeBatchException] should be thrownBy {
      doTheTest(dynBatchSize = Some(200000))
    }
  }

  it should "check that the table is mounted" in {
    a[TableNotMountedException] should be thrownBy {
      doTheTest(mountTable = false)
    }
  }

  it should "not write to the table with not matching schema" in {
    val thrown = the[Exception] thrownBy {
      val tableSchema = TableSchema.builder()
        .addValue("index", ColumnValueType.INT64)
        .addValue("content", ColumnValueType.STRING)
        .build()
      doTheTest(externalTableSchema = Some(tableSchema))
    }

    assertWriteFailed(thrown, "Cannot find YT columns for Spark fields")
  }

  it should "write the data to the table when dataframe contains some part of the table's columns" in {
    doTheTest(strictRowCheck = false, columnShift = 1, schemaModifier = { baseSchema =>
      TableSchema.builder()
        .addValue("extra_1", ColumnValueType.STRING)
        .addAll(baseSchema.getColumns)
        .addValue("extra_2", ColumnValueType.INT64)
        .addValue("extra_3", ColumnValueType.DOUBLE)
        .build()
    })
  }

  it should "not write the data to the table when dataframe does not contain all key columns" in {
    val thrown = the[Exception] thrownBy {
      doTheTest(schemaModifier = { baseSchema =>
        TableSchema.builder()
          .addKey("extra_key", ColumnValueType.INT64)
          .addAll(baseSchema.getColumns)
          .addValue("extra_value", ColumnValueType.STRING)
          .build()
      })
    }

    assertWriteFailed(thrown, "Cannot find Spark field for YT key column 'extra_key'")
  }

  it should "check that the saveMode is set to Append" in {
    an[AnalysisException] should be thrownBy {
      doTheTest(saveMode = SaveMode.ErrorIfExists)
    }
  }

  it should "write to a static table if it doesn't exist at specified path" in {
    doTheTest(createNewTable = false, mountTable = false)
  }

  it should "write null values to optional composite columns" in {
    val tableSchema = TableSchema.builder()
      .addKey("key", ColumnValueType.INT64)
      .addValue("dict", TiType.optional(TiType.dict(TiType.string(), TiType.int64())))
      .addValue("arr", TiType.optional(TiType.list(TiType.int64())))
      .addValue("nested", TiType.optional(TiType.struct(new Member("x", TiType.int64()))))
      .build()

    YtWrapper.createDynTable(tmpPath, tableSchema)
    YtWrapper.mountTableSync(tmpPath, Duration.ofMinutes(1))

    val schema = new StructType()
      .add("key", LongType, nullable = false)
      .add("dict", MapType(StringType, LongType), nullable = true)
      .add("arr", ArrayType(LongType), nullable = true)
      .add("nested", StructType(Seq(StructField("x", LongType))), nullable = true)

    val data = Seq(
      Row(1L, null, null, null),
      Row(2L, Map("a" -> 10L), Seq(1L, 2L), Row(7L))
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
        .key("dict").entity()
        .key("arr").entity()
        .key("nested").entity()
        .buildMap(),
      YTree.mapBuilder()
        .key("key").value(2L)
        .key("dict").value(
          YTree.listBuilder()
            .value(YTree.listBuilder().value("a").value(10L).buildList())
            .buildList())
        .key("arr").value(
          YTree.listBuilder().value(1L).value(2L).buildList())
        .key("nested").value(
          YTree.listBuilder().value(7L).buildList())
        .buildMap()
    )

    val resultRows: Seq[YTreeMapNode] = YtWrapper.selectRows(tmpPath)

    resultRows should contain theSameElementsAs expectedData
  }

  private def doTheTest(dataSize: Int = 10,
    dynBatchSize: Option[Int] = None,
    setInconsistentDynamicWrite: Boolean = true,
    createNewTable: Boolean = true,
    mountTable: Boolean = true,
    externalTableSchema: Option[TableSchema] = None,
    strictRowCheck: Boolean = true,
    columnShift: Int = 0,
    saveMode: SaveMode = SaveMode.Append,
    schemaModifier: TableSchema => TableSchema = identity
  ): Unit = {
    val sampleData = generateSampleData(dataSize)

    def writeDataLambda(): Unit = {
      import spark.implicits._

      val df = spark.createDataset(sampleData)

      if (createNewTable) {
        val tableSchema = if (externalTableSchema.isEmpty) {
          schemaModifier(new WriteSchemaConverter().tableSchema(df.schema, Unordered))
        } else {
          externalTableSchema.get
        }
        YtWrapper.createDynTable(tmpPath, tableSchema)
      }

      if (mountTable) {
        YtWrapper.mountTableSync(tmpPath, Duration.ofMinutes(1))
      }

      try {
        var dfWriter = df.write
          .format("yt")
          .mode(saveMode)

        if (setInconsistentDynamicWrite) {
          dfWriter = dfWriter.option("inconsistent_dynamic_write", "true")
        }

        dfWriter.save("ytTable:/" + tmpPath)
      } finally {
        if (mountTable) {
          YtWrapper.unmountTableSync(tmpPath, Duration.ofMinutes(1))
        }
      }
    }

    dynBatchSize match {
      case Some(dynBatchSizeVal) => withConf("spark.yt.write.dynBatchSize", dynBatchSizeVal.toString)(writeDataLambda())
      case None => writeDataLambda()
    }

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))

    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])
    outputPathAttributes("dynamic").boolValue() shouldBe createNewTable


    implicit val ytReadContext: YtReadContext = YtReadContext(yt, YtReadSettings.default)
    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData, strictRowCheck, columnShift)(_.getValues.get(0 + columnShift).stringValue())
  }
}

object YtDynamicTableWriterTest {
  case class SampleRow(key: String,
    value: String,
    score: Int,
    square: Long,
    cube: Long,
    root: Double,
    flag: Boolean,
    //TODO: deal with binary types - blob: Array[Byte],
    //TODO: deal with Timestamp type - ts: Timestamp
  )

  implicit val sampleRowOrdering: Ordering[SampleRow] = Ordering.by(_.key)

  def assertWriteFailed(thrown: Throwable, invariantSubstring: String): Unit = {
    var found = false
    var cur: Throwable = thrown
    while (cur != null) {
      if (cur.getMessage != null && cur.getMessage.contains(invariantSubstring)) {
        found = true
      }
      cur = cur.getCause
    }
    assert(found, s"Expected to find '$invariantSubstring' in exception cause chain")
  }

  def generateSampleData(limit: Int): Seq[SampleRow] = {
    (1 to limit).map { n =>
      val key = s"key_${Integer.toString(n, 36)}"
      SampleRow(key,
        "abracadabra",
        n,
        n.longValue() * n,
        n.longValue() * n * n,
        Math.sqrt(n),
        n % 2 == 0
        //TODO: deal with binary types - MD5Hash.digest(key).getDigest
        //TODO: deal with Timestamp type - new Timestamp(System.currentTimeMillis())
      )
    }
  }
}
