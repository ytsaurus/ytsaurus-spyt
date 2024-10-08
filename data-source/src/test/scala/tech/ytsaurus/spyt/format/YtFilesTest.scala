package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
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

class YtFilesTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  import spark.implicits._

  it should "write parquet files to yt" in {
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
    spark.read.parquet(s"file://$tmpLocalDir").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3)
  }

  // Ignored while YtFsInputStream's backward `seek` method is not implemented
  it should "read parquet files from yt" ignore {
    Seq(1, 2, 3).toDF.write.parquet(s"yt:/$tmpPath")

    spark.read.parquet(s"yt:/$tmpPath").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3)
  }

  it should "read csv" in {
    YtWrapper.createFile(tmpPath)
    val os = YtWrapper.writeFile(tmpPath, 1 minute, None)
    try {
      os.write(
        """a,b,c
          |1,2,3
          |4,5,6""".stripMargin.getBytes(StandardCharsets.UTF_8))
    } finally os.close()

    val res = spark.read.option("header", "true").csv(tmpPath.drop(1))

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row("1", "2", "3"),
      Row("4", "5", "6")
    )
  }

  it should "read large csv" in {
    writeFileFromResource("test.csv", tmpPath)
    spark.read.csv(s"yt:/$tmpPath").count() shouldEqual 100000
  }

  it should "read table from yt and write json files to external storage" in {
    val tableSchema = TableSchema.builder()
      .addValue("id", ColumnValueType.INT64)
      .addValue("value", ColumnValueType.STRING)
      .build()

    writeTableFromYson(Seq(
      """{id = 1; value = "value 1"}""",
      """{id = 2; value = "value 2"}""",
      """{id = 3; value = "value 3"}""",
    ), tmpPath, tableSchema)

    val df = spark.read.yt(tmpPath)
    val tmpDirPath = Files.createTempDirectory("test_parquet_write")
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
