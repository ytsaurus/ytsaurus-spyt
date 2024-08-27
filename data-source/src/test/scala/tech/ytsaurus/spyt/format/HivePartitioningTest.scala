package tech.ytsaurus.spyt.format

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper


class HivePartitioningTest extends AnyFlatSpec with TmpDir with LocalSpark with Matchers with TestUtils {
  import spark.implicits._

  private val ytSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("id", ColumnValueType.INT64)
    .addValue("name", ColumnValueType.STRING)
    .build()

  private val sparkSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType)
  ))

  private val partitionedSparkSchema = sparkSchema
    .add(StructField("external", IntegerType))
    .add(StructField("internal", IntegerType))

  it should "read partitioned data with recursiveFileLookup option set to false" in {
    createHivePartitionedTable()

    val df = spark.read.option("recursiveFileLookup", "false").yt(tmpPath)

    df.schema.toDDL shouldEqual partitionedSparkSchema.toDDL

    val sample = df.where($"external" === 5 and $"internal" === 2).select($"id").as[Long].collect()

    sample.length shouldBe 10
    sample should contain allElementsOf (520L to 529L)

    df.count() shouldBe 500L
  }

  it should "read partitioned data with recursiveFileLookup option set to true" in {
    createHivePartitionedTable()

    val df = spark.read.option("recursiveFileLookup", "true").yt(tmpPath)

    df.count() shouldBe 500L
    df.schema.toDDL shouldEqual sparkSchema.toDDL
  }

  it should "read hive partitioned data with basePath" in {
    createHivePartitionedTable()

    val df = spark.read.option("recursiveFileLookup", "false")
      .option("basePath", "ytTable:/" + tmpPath).yt(tmpPath + "/external=1")

    df.count() shouldBe 50L
    df.schema.toDDL shouldEqual partitionedSparkSchema.toDDL
  }

  it should "write hive partitioned data" in {
    val originDF = Seq(("alice", 1L), ("bob", 2L), ("alice", 3L)).toDF("name", "value")
    originDF.write.partitionBy("name").yt(tmpPath)

    val partitions = YtWrapper.listDir(tmpPath)
    partitions should contain theSameElementsAs Seq("name=alice", "name=bob")

    val df1 = spark.read.yt(tmpPath + "/name=alice")
    df1.collect() should contain theSameElementsAs Seq(Row(1L), Row(3L))

    val df2 = spark.read.yt(tmpPath + "/name=bob")
    df2.collect() should contain theSameElementsAs Seq(Row(2L))

    val df3 = spark.read.option("recursiveFileLookup", "false").yt(tmpPath)
    df3.collect() should contain theSameElementsAs Seq(Row(1L, "alice"), Row(2L, "bob"), Row(3L, "alice"))
  }

  it should "partition by few columns" in {
    val originDF = Seq(("alice", "s1", 1L), ("bob", "s1", 4L), ("bob", "s2", 2L), ("alice", "s3", 3L))
      .toDF("name", "surname", "value")
    originDF.write.partitionBy("surname", "name").yt(tmpPath)

    val partitions = YtWrapper.listDir(tmpPath)
    partitions should contain theSameElementsAs Seq("surname=s1", "surname=s2", "surname=s3")

    val partitionsS1 = YtWrapper.listDir(tmpPath + "/surname=s1")
    partitionsS1 should contain theSameElementsAs Seq("name=alice", "name=bob")

    val df1 = spark.read.option("recursiveFileLookup", "false").yt(tmpPath)
    df1.count() shouldBe 4

    val df2 = spark.read.option("recursiveFileLookup", "false").option("basePath", "ytTable:/" + tmpPath)
      .yt(tmpPath + "/surname=s2")
    df2.collect() should contain theSameElementsAs Seq(Row(2L, "s2", "bob"))
  }

  it should "support static partition overwrite" in {
    val df = Seq((1L, 1000), (2L, 2000), (2L, 1500), (2L, 2500)).toDF("id", "price")
    df.write.partitionBy("id").yt(tmpPath)

    val df2 = Seq((2L, 3000), (2L, 3500), (4L, 100)).toDF("id", "price")
    df2.write.mode(SaveMode.Overwrite).option("partitionOverwriteMode", "static").partitionBy("id").yt(tmpPath)

    val res = spark.read.option("recursiveFileLookup", "false").yt(tmpPath)
    res.collect() should contain theSameElementsAs Seq(Row(3000, 2L), Row(3500, 2L), Row(100, 4L))
  }

  it should "support dynamic partition overwrite" in {
    val df = Seq((1L, "11"), (2L, "14")).toDF("id", "place")
    df.write.partitionBy("id").yt(tmpPath)

    val df2 = Seq((2L, "15"), (2L, "16"), (0L, "17")).toDF("id", "place")
    df2.write.mode(SaveMode.Overwrite).option("partitionOverwriteMode", "dynamic").partitionBy("id").yt(tmpPath)

    val res = spark.read.option("recursiveFileLookup", "false").yt(tmpPath)
    res.collect() should contain theSameElementsAs Seq(Row("11", 1L), Row("15", 2L), Row("16", 2L), Row("17", 0L))
  }

  private def createHivePartitionedTable(): Unit = {
    YtWrapper.createDir(tmpPath)
    (1 to 10).foreach { external =>
      val pathPrefix = s"$tmpPath/external=$external"
      YtWrapper.createDir(pathPrefix)
      (1 to 5).foreach { internal =>
        val idPrefix = external*100 + internal*10
        val rows = (0 to 9).map { idSuffix =>
          val id = idPrefix + idSuffix
          s"""{id = $id; name = "Name for $id"}"""
        }
        val path = s"$pathPrefix/internal=$internal"
        writeTableFromYson(rows, path, ytSchema)
      }
    }
  }
}
