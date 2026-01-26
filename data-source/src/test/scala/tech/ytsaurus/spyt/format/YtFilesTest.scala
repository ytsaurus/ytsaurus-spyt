package tech.ytsaurus.spyt.format

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.stream.Collectors.toList
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

class YtFilesTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  override def reinstantiateSparkSession: Boolean = true

  private def testWriteParquet(_spark: SparkSession): Unit = {
    import _spark.implicits._
    Seq(1, 2, 3).toDF.write.parquet(s"yt:/$tmpPath")

    YtWrapper.isDir(tmpPath) shouldEqual true
    val files = YtWrapper.listDir(tmpPath)
    files.length shouldEqual 3
    files.foreach(name => name.endsWith(".parquet") shouldEqual true)

    val tmpLocalDir = Files.createTempDirectory("test_parquet")
    files.par.foreach { name =>
      val localPath = new File(tmpLocalDir.toFile, name).getPath
      YtWrapper.downloadFile(s"$tmpPath/$name", localPath)
    }
    _spark.read.parquet(s"file://$tmpLocalDir").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3)
  }

  it should "write parquet files to yt" in withSparkSession() { _spark =>
    testWriteParquet(_spark)
  }

  it should "use YtOutputCommitProtocol for writing parquet even if spark.yt.write.distributed.enabled is true" in
    withSparkSession(Map("spark.yt.write.distributed.enabled" -> "true")) { _spark =>
      testWriteParquet(_spark)
  }

  // Ignored while YtFsInputStream's backward `seek` method is not implemented
  it should "read parquet files from yt" ignore withSparkSession() { _spark =>
    import _spark.implicits._
    Seq(1, 2, 3).toDF.write.parquet(s"yt:/$tmpPath")

    _spark.read.parquet(s"yt:/$tmpPath").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3)
  }

  it should "read csv" in withSparkSession() { _spark =>
    YtWrapper.createFile(tmpPath)
    val os = YtWrapper.writeFile(tmpPath, 1 minute, None)
    try {
      os.write(
        """a,b,c
          |1,2,3
          |4,5,6""".stripMargin.getBytes(StandardCharsets.UTF_8))
    } finally os.close()

    val res = _spark.read.option("header", "true").csv(tmpPath.drop(1))

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row("1", "2", "3"),
      Row("4", "5", "6")
    )
  }

  it should "read large csv" in withSparkSession() { _spark =>
    writeFileFromResource("test.csv", tmpPath)
    _spark.read.csv(s"yt:/$tmpPath").count() shouldEqual 100000
  }

  it should "read table from yt and write json files to external storage" in withSparkSession() { _spark =>
    val tableSchema = TableSchema.builder()
      .addValue("id", ColumnValueType.INT64)
      .addValue("value", ColumnValueType.STRING)
      .build()

    writeTableFromYson(Seq(
      """{id = 1; value = "value 1"}""",
      """{id = 2; value = "value 2"}""",
      """{id = 3; value = "value 3"}""",
    ), tmpPath, tableSchema)

    val df = _spark.read.yt(tmpPath)
    val tmpDirPath = Files.createTempDirectory("test_json_write_to_local_filesystem")
    val resultPath = tmpDirPath.resolve("result")

    df.write.json(f"file://${resultPath}")

    Files.list(resultPath).filter(_.getFileName.toString == "_SUCCESS").count() shouldBe 1L
    val result = Files.list(resultPath)
      .filter(_.toString.endsWith(".json"))
      .flatMap(p => Files.readAllLines(p).stream())
      .sorted()
      .collect(toList[String]).asScala

    result should contain theSameElementsInOrderAs Seq(
      """{"id":1,"value":"value 1"}""",
      """{"id":2,"value":"value 2"}""",
      """{"id":3,"value":"value 3"}"""
    )
  }
}
