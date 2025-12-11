package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.Random

class YtFilePartitionTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  behavior of "YtFilePartition"

  import TestPartitionedFile._

  it should "order partitionedFiles using partitionedFilesOrdering" in {
    val expected = Seq(
      Dynamic("//path0", 1), // ordered by path

      Static("//path1", 0, 10), // grouped by path and ordered by beginRow
      Static("//path1", 10, 20),
      Static("//path1", 30, 40),

      Static("//path2", 0, 100), // grouped by path and ordered by beginRow
      Static("//path2", 100, 200),

      Dynamic("//path3", 2), // ordered by path

      Csv("//path4", 5), // ordered by length descending
      Csv("//path5", 4)
    )

    val actual = Random.shuffle(expected.map(_.toPartitionedFile))
      .sorted(YtFilePartition.partitionedFilesOrdering)
      .map(fromPartitionedFile)

    actual should contain theSameElementsInOrderAs expected
  }

  private val MB = 1024L * 1024
  private val GB = 1024 * MB

  private val standardConfig = YtFilePartition.PartitioningConfig(
    defaultMaxSplitBytes = 2 * GB,        // YT default
    minSplitBytes = 1 * GB,               // YT default
    openCostInBytes = 4 * MB,             // Spark default
    defaultParallelism = 8
  )

  it should "use min split bytes (1GB) for small tables" in {
    val partitions = createPartitions(Seq(100 * MB, 200 * MB, 300 * MB))

    val result = YtFilePartition.maxSplitBytes(partitions, None, standardConfig)

    result shouldBe 1 * GB // Should enforce minimum
  }

  it should "use bytes per core for medium-sized chunks" in {
    val fileSize = 600 * MB
    val numFiles = 16
    val partitions = createPartitions(Seq.fill(numFiles)(fileSize))

    val totalBytes = numFiles * (fileSize + standardConfig.openCostInBytes)
    val bytesPerCore = totalBytes / standardConfig.defaultParallelism

    val result = YtFilePartition.maxSplitBytes(partitions, None, standardConfig)

    // bytesPerCore = (16 * 604MB) / 8 = 1208MB
    result shouldBe bytesPerCore
  }

  it should "use default max split bytes for large tables with big chunks" in {
    val fileSize = 10 * GB
    val numFiles = 100
    val partitions = createPartitions(Seq.fill(numFiles)(fileSize))

    val result = YtFilePartition.maxSplitBytes(partitions, None, standardConfig)

    // bytesPerCore = ~1000GB / 8 = 125GB
    // min(2GB, 125GB) = 2GB, max(1GB, 2GB) = 2GB
    result shouldBe standardConfig.defaultMaxSplitBytes
  }

  it should "use custom read parallelism to calculate split size" in {
    val fileSize = 5 * GB
    val numFiles = 20
    val customParallelism = 50
    val partitions = createPartitions(Seq.fill(numFiles)(fileSize))

    val totalBytes = numFiles * (fileSize + standardConfig.openCostInBytes)
    val expected = totalBytes / customParallelism + 1

    val result = YtFilePartition.maxSplitBytes(partitions, Some(customParallelism), standardConfig)

    result shouldBe expected
  }

  private def createPartitions(fileSizes: Seq[Long]): Seq[PartitionDirectory] = {
    val fileStatuses = fileSizes.zipWithIndex.map { case (size, idx) =>
      new FileStatus(
        size,
        false, // isdir
        1,     // block_replication
        128 * MB, // blocksize
        System.currentTimeMillis(),
        new Path(s"//tmp/test/file_$idx")
      )
    }.toArray
    Seq(PartitionDirectory(InternalRow.empty, fileStatuses))
  }

}
