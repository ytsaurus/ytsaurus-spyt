package tech.ytsaurus.spyt.format

import org.apache.spark.sql.v2.Utils.getParsedKeys
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import scala.util.Random

class YtStaticPartitioningTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir
  with TestUtils with YtDistributedReadingTestUtils {

  behavior of "YtStaticPartitioning"

  private val basePartitioningConf = Map(
    s"spark.yt.${SparkYtConfiguration.Read.YtPartitioningEnabled.name}" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb"
  )

  private def partitioningConf(compressedSizeEnabled: Boolean): Map[String, String] =
    basePartitioningConf + (
      s"spark.yt.${SparkYtConfiguration.Read.YtPartitioningCompressedSizeEnabled.name}" ->
        compressedSizeEnabled.toString
      )

  private val RowCount = 3000
  private val MinBlobLength = 200
  private val BlobLengthRange = 300
  private val Seed = 42

  private val wideSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .addValue("flag", ColumnValueType.INT64)
    .addValue("blob1", ColumnValueType.STRING)
    .addValue("blob2", ColumnValueType.STRING)
    .addValue("blob3", ColumnValueType.STRING)
    .addValue("blob4", ColumnValueType.STRING)
    .build()

  private def generateWideYsonRows(rowCount: Int): Seq[String] = {
    val rng = new Random(Seed)
    (1 to rowCount).map { i =>
      // Use varying blob lengths to make compression less uniform
      val blobLength = MinBlobLength + rng.nextInt(BlobLengthRange)
      val flag = i % 2 == 0
      val blob = Seq("A", "B", "C", "D").map(ch => s""""${ch * blobLength}"""")
      s"""{id = $i; flag = $flag; blob1 = ${blob(0)}; blob2 = ${blob(1)}; """ +
        s"""blob3 = ${blob(2)}; blob4 = ${blob(3)}}"""
    }
  }

  private def countPartitions(conf: Map[String, String]): Int =
    withConfs(conf) {
      val df = spark.read.yt(tmpPath).select("flag")
      getParsedKeys(df).length
    }

  it should "produce more partitions with compressed size mode because it closer for actual transfer volume" in {
    writeTableFromYson(generateWideYsonRows(RowCount), tmpPath, wideSchema, OptimizeMode.Lookup)

    val compressedSizePartitions = countPartitions(partitioningConf(compressedSizeEnabled = true))
    val dataWeightPartitions = countPartitions(partitioningConf(compressedSizeEnabled = false))

    withClue(
      s"Compressed size partitioning ($compressedSizePartitions partitions) should produce " +
        s"at least 2x more partitions than data weight ($dataWeightPartitions), " +
        s"because compressed size reflects actual network transfer volume for Lookup tables"
    ) {
      compressedSizePartitions should be > 2*dataWeightPartitions
    }
  }
}
