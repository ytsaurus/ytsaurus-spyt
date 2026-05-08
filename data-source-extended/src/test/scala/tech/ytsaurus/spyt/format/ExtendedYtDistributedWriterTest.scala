package tech.ytsaurus.spyt.format

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnSchema, ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.{YtReader, YtWriter}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.jdk.CollectionConverters._

class ExtendedYtDistributedWriterTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils {
  behavior of "DistributedWriteOutputCommitProtocol"

  override def reinstantiateSparkSession: Boolean = true

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.yt.write.distributed.enabled", "true")
  }

  private def appendTestTemplate(_spark: SparkSession, columns: List[ColumnSchema], data: Array[Array[String]]): Unit = {
    val basePath = tmpPath + "/test"
    val dataToAppend = tmpPath + "/toAppend"

    _spark.range(10).select(col("id"), concat(lit("it = "), col("id")).as("a")).write.yt(basePath)

    val ysonSeq: Seq[String] = data.map { row =>
      val pairs = columns.zip(row).map { element =>
        s"${element._1.getName} = ${element._2}"
      }
      "{" + pairs.mkString("; ") + "}"
    }

    writeTableFromYson(ysonSeq, dataToAppend, TableSchema.builder().addAll(columns.asJava).build())
    _spark.read.yt(dataToAppend).createOrReplaceTempView("toAppend")

    _spark.sql("SELECT * FROM toAppend").write.mode("append").yt(basePath)
  }

  it should "append data to an existing table" in withSparkSession() { _spark =>
    _spark.range(100).write.yt(tmpPath)
    _spark.range(100, 200).write.mode(SaveMode.Append).yt(tmpPath)

    val writtenData = readTableAsYson(tmpPath).map(_.asMap().get("id").longValue())
    writtenData should contain theSameElementsAs (0L to 199L)
  }

  it should "append table with different columns order" in withSparkSession() { _spark =>
    import _spark.implicits._

    val columns: List[ColumnSchema] = List(ColumnSchema.builder("id", ColumnValueType.INT64).build(),
      ColumnSchema.builder("a", ColumnValueType.STRING).build)

    val data: Array[Array[String]] =  Array(
      Array("101", "one"),
      Array("102", "two")
    )

    appendTestTemplate(_spark, columns, data)
  }

  it should "throw exception when append table with different columns names" in withSparkSession(){ _spark =>
    import _spark.implicits._

    val columns: List[ColumnSchema] = List(ColumnSchema.builder("c", ColumnValueType.INT64).build(),
      ColumnSchema.builder("d", ColumnValueType.INT64).build)

    val data: Array[Array[String]] =  Array(
      Array("101", "101"),
      Array("102", "102")
    )

    assertThrows[IllegalArgumentException](appendTestTemplate(_spark, columns, data))
  }

  it should "append table with smaller amount of columns" in withSparkSession(){ _spark =>
    import _spark.implicits._

    val columns: List[ColumnSchema] = List(ColumnSchema.builder("id", ColumnValueType.INT64).build())

    val data: Array[Array[String]] =  Array(
      Array("101"),
      Array("102")
    )

    appendTestTemplate(_spark, columns, data)
  }

  it should "write custom options to attributes with append" in withSparkSession() { _spark =>
    import _spark.implicits._

    val sample = _spark.range(50).select(col("id"), concat(lit("id = "), col("id")).as("a"))

    sample.write.option("attr_custom_string", "before_append").yt(tmpPath)

    val toAppend = _spark.range(51, 100).select(col("id"), concat(lit("id = "), col("id")).as("a"))
    toAppend.write
      .option("attr_custom_string", "after_append")
      .option("attr_new_attribute", 10)
      .mode(SaveMode.Append)
      .yt(tmpPath)

    val outputPathAttributes = YtWrapper.attributes(tmpPath, None, Set.empty[String])

    outputPathAttributes("new_attribute").intValue() shouldBe 10
    outputPathAttributes("custom_string").stringValue() shouldBe "after_append"
  }
}
