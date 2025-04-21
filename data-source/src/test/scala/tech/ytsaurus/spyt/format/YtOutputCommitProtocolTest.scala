package tech.ytsaurus.spyt.format

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.FileCommitProtocol
import org.scalatest.FlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.util.Random

class YtOutputCommitProtocolTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {

  override def numFailures: Int = 4

  override def reinstantiateSparkSession: Boolean = true

  override def afterAll(): Unit = {
    LocalSpark.stop()
    super.afterAll()
  }

  behavior of "YtOutputCommitProtocol"

  it should "not duplicate output data in case of failures after commiting a task" in {
    withConfs(Map(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "false",
      "spark.sql.sources.commitProtocolClass" -> "tech.ytsaurus.spyt.format.BogusYtOutputCommitProtocol"
    )) {
      val mainTableSchema = TableSchema.builder()
        .addValue("id", ColumnValueType.INT64)
        .addValue("join_id", ColumnValueType.INT64)
        .addValue("value", ColumnValueType.STRING)
        .build()

      val joinTableSchema = TableSchema.builder()
        .addValue("id", ColumnValueType.INT64)
        .addValue("value", ColumnValueType.STRING)
        .build()

      YtWrapper.createDir(tmpPath)

      val mainTablePath = s"$tmpPath/main_table"
      val mainTableRows = 10000
      val joinTablePath = s"$tmpPath/join_table"
      val joinTableRows = 1000
      val outTablePath = s"$tmpPath/out"

      writeTableFromYson(
        (1 to mainTableRows).map(id => s"""{id = ${id}u; join_id = ${id*2}u; value = "value $id"}"""),
        mainTablePath,
        mainTableSchema
      )

      writeTableFromYson(
        (1 to joinTableRows).map(id => s"""{id = ${id*20}u; value = "value $id"}"""),
        joinTablePath,
        joinTableSchema
      )

      val mainTableDf = spark.read.yt(mainTablePath).repartition(13)
      val joinTableDf = spark.read.yt(joinTablePath).repartition(7)

      val result = mainTableDf.join(joinTableDf, mainTableDf("join_id") === joinTableDf("id"), "left")

      result.select(mainTableDf("id"), joinTableDf("value")).write.yt(outTablePath)

      val rowCount = YtWrapper.attribute(outTablePath, "row_count").intValue()
      val chunkCount = YtWrapper.attribute(outTablePath, "chunk_count").intValue()

      rowCount shouldBe mainTableRows
      chunkCount shouldEqual spark.conf.get("spark.sql.shuffle.partitions").toInt
    }
  }
}

class BogusYtOutputCommitProtocol(jobId: String, outputPath: String, dynamicPartitionOverwrite: Boolean)
  extends YtOutputCommitProtocol(jobId, outputPath, dynamicPartitionOverwrite) {
  import BogusYtOutputCommitProtocol._

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    val res = super.commitTask(taskContext)
    if (pseudoRandom.nextInt(5) == 0) {
      throw new RuntimeException("BOOOOM!!!!")
    }
    res
  }
}

object BogusYtOutputCommitProtocol {
  // We need a global pseudo-random sequence here so the test would be deterministic
  // With seed of 3 we expect that commitTask will fail 3 times
  val pseudoRandom = new Random(3)
}