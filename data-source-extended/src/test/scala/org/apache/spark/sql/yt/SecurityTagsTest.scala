package org.apache.spark.sql.yt

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.FileStatusCache
import org.apache.spark.sql.functions.{col, count, lit, udf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.{YtReader, YtWriter}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.YtSecurityTags
import tech.ytsaurus.ysontree.YTreeTextSerializer

import java.nio.file.Paths
import java.time.Duration

class SecurityTagsTest extends AnyFlatSpec with Matchers with LocalSpark with TestUtils with TmpDir {

  override def reinstantiateSparkSession: Boolean = true

  private def createInput(name: String, tags: String = "[source;shared]"): String = {
    val path = s"$tmpPath/$name"
    YtWrapper.createDir(Paths.get(path).getParent.toString, ignoreExisting = true)
    val schema = TableSchema.builder().addValue("id", ColumnValueType.INT64).build()
    writeTableFromYson((0L until 4L).map(id => s"{id = $id}"), path, schema)
    YtWrapper.setAttribute(path, YtSecurityTags.attribute, YTreeTextSerializer.deserialize(tags))
    path
  }

  private def tags(path: String): Seq[String] = {
    YtSecurityTags.fromNode(YtWrapper.attribute(path, YtSecurityTags.attribute))
  }

  private def output: String = s"$tmpPath/output"

  Seq(false, true).foreach { distributed =>
    val settings = Map("spark.yt.write.distributed.enabled" -> distributed.toString)

    it should s"inherit the union through joins, projections and UDFs (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        val left = session.read.yt(createInput("left"))
        val right = session.read.yt(createInput("right", "[other;shared]"))
        val identity = udf((value: Long) => value)
        left.join(right, "id").select(identity(col("id")).as("id"))
          .union(session.range(1).toDF()).write.yt(output)
        tags(output) shouldBe Seq("other", "shared", "source")
      }
    }

    it should s"preserve provenance for optimized counts and empty results (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        val input = session.read.yt(createInput("input"))
        input.agg(count(lit(1)).as("count")).write.yt(output)
        tags(output) shouldBe Seq("shared", "source")
        input.filter(lit(false)).write.mode("overwrite").yt(output)
        tags(output) shouldBe Seq("shared", "source")
        session.read.yt(output).count() shouldBe 0L
      }
    }

    it should s"retain cached input tags after source deletion (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        val source = createInput("input")
        val cached = session.read.yt(source).cache()
        try {
          cached.count() shouldBe 4L
          YtWrapper.remove(source)
          cached.createOrReplaceTempView("tagged_cache")
          session.sql("SELECT id + 1 AS id FROM tagged_cache").write.yt(output)
          tags(output) shouldBe Seq("shared", "source")
        } finally {
          session.catalog.dropTempView("tagged_cache")
          cached.unpersist()
        }
      }
    }

    it should s"inherit tags in SQL CTAS, INSERT and subqueries (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        val source = createInput("input")
        val other = createInput("other", "[other]")
        session.sql(s"CREATE TABLE yt.`$output` USING yt AS SELECT * FROM yt.`$source`")
        tags(output) shouldBe Seq("shared", "source")
        session.sql(s"INSERT INTO yt.`$output` SELECT * FROM yt.`$other`")
        tags(output) shouldBe Seq("other", "shared", "source")
        session.sql(s"INSERT OVERWRITE yt.`$output` SELECT * FROM yt.`$other`")
        tags(output) shouldBe Seq("other")
        session.sql(s"SELECT 1L AS id WHERE EXISTS (SELECT * FROM yt.`$source`)")
          .write.mode("overwrite").yt(output)
        tags(output) shouldBe Seq("shared", "source")
      }
    }

    it should s"discover tags in directories with a supplied schema (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        createInput("inputs/first")
        createInput("inputs/nested/second", "[other]")
        session.read.schema("id LONG").yt(s"$tmpPath/inputs").write.yt(output)
        tags(output) shouldBe Seq("other", "shared", "source")
      }
    }

    it should s"preserve existing tags on append and replace them on overwrite (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        val source = session.read.yt(createInput("input"))
        session.range(1).write.option("security_tags", "[existing]").yt(output)
        source.write.mode("append").yt(output)
        tags(output) shouldBe Seq("existing", "shared", "source")
        session.range(1).write.mode("append").yt(output)
        tags(output) shouldBe Seq("existing", "shared", "source")
        source.write.mode("overwrite").yt(output)
        tags(output) shouldBe Seq("shared", "source")
        session.range(1).write.mode("overwrite").yt(output)
        tags(output) shouldBe empty
      }
    }

    Seq(
      "security_tags" -> Map("attr_security_tags" -> "[ignored]"),
      "attr_security_tags" -> Map.empty[String, String]).foreach { case (option, fallbackOptions) =>
      it should s"honor explicit overrides using $option (distributed=$distributed)" in {
        withSparkSession(settings) { session =>
          val source = session.read.yt(createInput("input"))
          val writer = source.write.options(fallbackOptions).option(option, "[override;override]")
          writer.yt(output)
          tags(output) shouldBe Seq("override")
          YtWrapper.setAttribute(output, YtSecurityTags.attribute, YtSecurityTags.toNode(Seq("existing")))
          writer.mode("append").yt(output)
          tags(output) shouldBe Seq("existing", "override")
          writer.option(option, "[]").mode("append").yt(output)
          tags(output) shouldBe Seq("existing", "override")
          writer.mode("overwrite").yt(output)
          tags(output) shouldBe empty
          writer.option(option, "[override]").mode("overwrite").yt(output)
          tags(output) shouldBe Seq("override")
        }
      }
    }

    it should s"keep tags through sorted table commits (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        val source = session.read.yt(createInput("input"))
          .repartitionByRange(2, col("id")).sortWithinPartitions("id")
        source.write.sortedBy("id").yt(output)
        tags(output) shouldBe Seq("shared", "source")
        source.select((col("id") + 10).as("id")).write.sortedBy("id")
          .option("security_tags", "[new]").mode("append").yt(output)
        tags(output) shouldBe Seq("new", "shared", "source")
      }
    }

    it should s"roll back tags together with a failed write (distributed=$distributed)" in {
      withSparkSession(settings) { session =>
        session.range(1).write.option("security_tags", "[existing]").yt(output)
        val source = session.read.yt(createInput("input"))
        val failWrite = udf((value: Long) => {
          require(value < 0, "The test deliberately fails the write.")
          value
        })
        val error = intercept[SparkException] {
          source.select(failWrite(col("id")).as("id")).write.mode("overwrite").yt(output)
        }
        error.getMessage should include("The test deliberately fails the write.")
        tags(output) shouldBe Seq("existing")
        session.read.yt(output).count() shouldBe 1L
      }
    }
  }

  it should "propagate tags to every Hive partition" in {
    withSparkSession(Map("spark.yt.write.distributed.enabled" -> "false")) { session =>
      session.read.yt(createInput("input")).withColumn("part", col("id") % 2)
        .write.partitionBy("part").yt(output)
      tags(s"$output/part=0") shouldBe Seq("shared", "source")
      tags(s"$output/part=1") shouldBe Seq("shared", "source")
    }
  }

  it should "preserve tags during parallel input discovery" in {
    withSparkSession(Map(
      "spark.yt.read.listParentDirectories" -> "false",
      "spark.sql.sources.parallelPartitionDiscovery.threshold" -> "1")) { session =>
      val first = createInput("first/table")
      val second = createInput("second/table", "[other]")
      session.read.yt(first, second).write.yt(output)
      tags(output) shouldBe Seq("other", "shared", "source")
    }
  }

  it should "read security tags from the input transaction" in {
    withSparkSession() { session =>
      val source = createInput("input")
      val transaction = YtWrapper.createTransaction(None, Duration.ofMinutes(2))
      try {
        val transactionId = transaction.getId.toString
        YtWrapper.lockNodeAsync(YPath.simple(source), transactionId).join()
        YtWrapper.setAttribute(source, YtSecurityTags.attribute, YtSecurityTags.toNode(Seq("changed")))
        session.read.option("transaction", transactionId).yt(source).write.yt(output)
        tags(output) shouldBe Seq("shared", "source")
        tags(source) shouldBe Seq("changed")
      } finally {
        transaction.abort().join()
      }
    }
  }

  it should "inherit tags from V1 sources" in {
    withSparkSession(Map("spark.sql.sources.useV1SourceList" -> "yt")) { session =>
      createInput("inputs/first")
      createInput("inputs/second", "[other]")
      session.read.option("recursiveFileLookup", "true").yt(s"ytTable:/$tmpPath/inputs").write.yt(output)
      tags(output) shouldBe Seq("other", "shared", "source")
    }
  }

  it should "use cached provenance when a new query reuses data with changed source tags" in {
    withSparkSession() { session =>
      val source = createInput("input")
      val cached = session.read.yt(source).cache()
      try {
        cached.count() shouldBe 4L
        YtWrapper.setAttribute(source, YtSecurityTags.attribute, YtSecurityTags.toNode(Seq("changed")))
        FileStatusCache.getOrCreate(session).invalidateAll()
        val fresh = session.read.yt(source)
        fresh.queryExecution.withCachedData.exists(_.isInstanceOf[InMemoryRelation]) shouldBe true
        fresh.write.yt(output)
        tags(output) shouldBe Seq("changed", "shared", "source")
        cached.unpersist(blocking = true)
        fresh.write.mode("overwrite").yt(output)
        tags(output) shouldBe Seq("changed", "shared", "source")
      } finally {
        cached.unpersist()
      }
    }
  }
}
