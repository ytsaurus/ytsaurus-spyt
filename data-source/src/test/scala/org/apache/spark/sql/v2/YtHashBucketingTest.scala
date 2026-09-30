package org.apache.spark.sql.v2

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.expressions.{Expressions, FieldReference, LogicalExpressions, NamedReference}
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, LongType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.format.bucketing.HashBucketingTestUtils
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.typeinfo.TiType

class YtHashBucketingTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir
  with HashBucketingTestUtils {
  behavior of "YtScan"

  private val buckets = 10
  private val valueFactor = 10L

  private val keys = 1L to 24L
  private val keyRows: Seq[(Long, Long)] = keys.map(key => (key, key * valueFactor))
  private val keyYsonRows = keyRows.map { case (key, value) => s"{$keyColumn=$key;$valueColumn=$value}" }
  private val keyColumns = Seq(keyColumn -> int64Column)
  private val expression = s"farm_hash($keyColumn) % $buckets"

  private val bucketedTable = s"$tmpPath-bucketed"
  private val oneBucketTable = s"$tmpPath-one-bucket"
  private val stringKeyTable = s"$tmpPath-string-key"

  private val maxBucketsKey = s"spark.yt.${Read.HashBucketing.MaxBuckets.name}"
  private val catalogReadOption = SparkYtInternalConfiguration.HashBucketingCatalogRead.name
  private val keyHint = sessionOption(s"${keyColumn}_hint")

  private def writeHashedTable(
    path: String,
    expression: String,
    columns: Seq[(String, TiType)],
    rows: Seq[String],
    sortBy: Seq[String] = Nil,
    optimizeForScan: Boolean = false): Unit = {
    awaitTables(startHashedTable(path, expression, columns, rows, sortBy, optimizeForScan))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    awaitTables(
      startBucketedKeyTable(bucketedTable, buckets, keys, valueFactor),
      startBucketedKeyTable(oneBucketTable, 1, keys, valueFactor),
      startHashedTable(
        stringKeyTable,
        expression,
        Seq(keyColumn -> stringColumn),
        keyRows.map { case (key, value) => s"""{$keyColumn="$key";$valueColumn=$value}""" }))
  }

  private def shouldNotBeBucketed(scan: YtScan): Unit = {
    scan.outputPartitioning() shouldBe an[UnknownPartitioning]
    scan.getPartitions.filter(_.isInstanceOf[YtKeyedFilePartition]) shouldBe empty
  }

  private def scanRow(row: Row): (Long, Long, Long) = {
    (
      row.getAs[java.math.BigDecimal](hashColumn).longValueExact(),
      row.getAs[Long](keyColumn),
      row.getAs[Long](valueColumn))
  }

  private def keyValueRows(df: DataFrame): Seq[(Long, Long)] = {
    df.collect().toSeq.map(row => (row.getAs[Long](keyColumn), row.getAs[Long](valueColumn)))
  }

  private def writeKeyTable(path: String, sorted: Boolean): Unit = {
    val builder = TableSchema.builder().setUniqueKeys(false)
    val withKey = if (sorted) {
      builder.addKey(keyColumn, ColumnValueType.INT64)
    } else {
      builder.addValue(keyColumn, ColumnValueType.INT64)
    }
    writeTableFromYson(keyYsonRows, path, withKey.addValue(valueColumn, ColumnValueType.INT64).build())
  }

  it should "report key grouped partitioning for a farm_hash % N bucketed table" in {
    withConfs(bucketingConf) {
      val scan = ytScan(catalogTable(bucketedTable))
      val partitioning = keyGroupedPartitioning(scan)
      partitioning.keys().toSeq shouldEqual Seq(Expressions.bucket(buckets, keyColumn))
      partitioning.numPartitions() shouldBe buckets
      scan.getPartitions.length shouldBe buckets
    }
  }

  it should "read each bucket into its own partition keyed by the bucket value" in {
    withConfs(bucketingConf) {
      val df = catalogTable(bucketedTable)
      shouldKeyPartitionsByBucket(ytScan(df), buckets)
      val stored = storedRows(bucketedTable)
      stored.map { case (_, key, value) => (key, value) } should contain theSameElementsAs keyRows
      shouldReadRowsIntoTheirBuckets(df, stored)(scanRow)
    }
  }

  it should "read a bucketed table optimized for scan in Arrow batches with every row in its bucket" in {
    val table = s"$tmpPath-scan-optimized"
    writeHashedTable(table, expression, keyColumns, keyYsonRows, optimizeForScan = true)
    withConfs(bucketingConf) {
      val df = catalogTable(table).select(keyColumn, valueColumn)
      val scan = ytScan(df)
      keyGroupedPartitioning(scan).numPartitions() shouldBe buckets
      scan.createReaderFactory().supportColumnarReads(scan.getPartitions.head) shouldBe true
      val stored = storedRows(table)
      val bucketOfKey = stored.map { case (hash, key, _) => key -> hash }.toMap
      shouldReadRowsIntoTheirBuckets(df, stored) { row =>
        val key = row.getAs[Long](keyColumn)
        (bucketOfKey(key), key, row.getAs[Long](valueColumn))
      }
    }
  }

  it should "bucket a scan that does not read the hash column" in {
    withConfs(bucketingConf) {
      val df = catalogTable(bucketedTable).select(keyColumn, valueColumn)
      val scan = ytScan(df)
      keyGroupedPartitioning(scan).keys().toSeq shouldEqual Seq(Expressions.bucket(buckets, keyColumn))
      scan.getPartitions.length shouldBe buckets
      keyValueRows(df) should contain theSameElementsAs keyRows
    }
  }

  it should "bucket a table with a single bucket into one partition" in {
    withConfs(bucketingConf) {
      val df = catalogTable(oneBucketTable)
      val scan = ytScan(df)
      keyGroupedPartitioning(scan).keys().toSeq shouldEqual Seq(Expressions.bucket(1, keyColumn))
      scan.getPartitions.length shouldBe 1
      shouldReadRowsIntoTheirBuckets(df, storedRows(oneBucketTable))(scanRow)
    }
  }

  it should "bucket a table with no more buckets than the configured maximum into its own number of buckets" in {
    Seq(buckets, Int.MaxValue).foreach { maxBuckets =>
      withConfs(bucketingConf ++ Map(maxBucketsKey -> maxBuckets.toString)) {
        keyGroupedPartitioning(ytScan(catalogTable(bucketedTable))).numPartitions() shouldBe buckets
      }
    }
  }

  it should "read empty buckets of a table sorted by the hash column only" in {
    val table = s"$tmpPath-hash-only-key"
    writeHashedTable(table, expression, keyColumns, keyYsonRows.take(2), Seq(hashColumn))
    withConfs(bucketingConf) {
      val df = catalogTable(table)
      ytScan(df).getPartitions.length shouldBe buckets
      storedRows(table).map(_._1).distinct.length should be < buckets
      shouldReadRowsIntoTheirBuckets(df, storedRows(table))(scanRow)
    }
  }

  it should "bucket an empty table into empty partitions" in {
    val table = s"$tmpPath-empty"
    createEmptyHashedTable(table, expression, keyColumns)
    withConfs(bucketingConf) {
      val df = catalogTable(table)
      ytScan(df).getPartitions.length shouldBe buckets
      df.collect() shouldBe empty
    }
  }

  it should "reference a hash argument whose name is not a plain identifier" in {
    val (table, quotedColumn) = (s"$tmpPath-quoted-column", "user-id")
    writeHashedTable(
      table,
      s"farm_hash(`$quotedColumn`) % $buckets",
      Seq(quotedColumn -> int64Column),
      keyRows.map { case (key, value) => s"""{"$quotedColumn"=$key;$valueColumn=$value}""" })
    withConfs(bucketingConf) {
      val df = catalogTable(table)
      keyGroupedPartitioning(ytScan(df)).keys().toSeq shouldEqual
        Seq(LogicalExpressions.bucket(buckets, Array[NamedReference](FieldReference.column(quotedColumn))))
      df.collect().map(_.getAs[Long](quotedColumn)) should contain theSameElementsAs keys
    }
  }

  it should "apply pushed down key filters within the buckets" in {
    withConfs(bucketingConf ++ Map(s"spark.yt.${Read.KeyColumnsFilterPushdown.Enabled.name}" -> "true")) {
      val stored = storedRows(bucketedTable)
      val (bucket, key, _) = stored.head
      Seq(
        catalogTable(bucketedTable).filter(col(keyColumn) === key) -> stored.filter(_._2 == key),
        catalogTable(bucketedTable).filter(col(hashColumn) === bucket) -> stored.filter(_._1 == bucket)
      ).foreach { case (df, expected) =>
        ytScan(df).getPartitions.length shouldBe buckets
        shouldReadRowsIntoTheirBuckets(df, expected)(scanRow)
      }
    }
  }

  it should "fail to plan a catalog read of a bucketed table when the maximum of buckets is not a positive number" in {
    val name = Read.HashBucketing.MaxBuckets.name
    Seq("0", "-1", "many").foreach { maxBuckets =>
      withConfs(bucketingConf ++ Map(maxBucketsKey -> maxBuckets)) {
        withClue(s"[$maxBuckets] ") {
          val error = the[IllegalArgumentException] thrownBy ytScan(catalogTable(bucketedTable))
          error.getMessage should include(s"spark.ytsaurus.$name (or spark.yt.$name)")
          if (maxBuckets == "many") {
            error.getCause shouldBe a[NumberFormatException]
          } else {
            error.getMessage should include(maxBuckets)
          }
        }
      }
    }
  }

  it should "read a table without a hash column whatever the maximum of buckets" in {
    val table = s"$tmpPath-plain-sorted"
    writeKeyTable(table, sorted = true)
    Seq("0", "-1", "many").foreach { maxBuckets =>
      withConfs(bucketingConf ++ Map(maxBucketsKey -> maxBuckets)) {
        withClue(s"[$maxBuckets] ") {
          val df = catalogTable(table)
          shouldNotBeBucketed(ytScan(df))
          keyValueRows(df) should contain theSameElementsAs keyRows
        }
      }
    }
  }

  it should "not validate the maximum of buckets when hash bucketing is disabled or the table is read by path" in {
    withConfs(bucketingConf ++ Map(hashBucketingEnabledKey -> "false", maxBucketsKey -> "0")) {
      shouldNotBeBucketed(ytScan(catalogTable(bucketedTable)))
    }
    withConfs(bucketingConf ++ Map(maxBucketsKey -> "0")) {
      shouldNotBeBucketed(ytScan(spark.read.yt(bucketedTable)))
    }
  }

  it should "not bucket a table hashed by several columns" in {
    val (table, regionColumn, userColumn) = (s"$tmpPath-multi-column", "region", "user_id")
    writeHashedTable(
      table,
      s"farm_hash($userColumn, $regionColumn) % $buckets",
      Seq(regionColumn -> stringColumn, userColumn -> int64Column),
      keys.map(key => s"""{$regionColumn="r${key % 3}";$userColumn=$key;$valueColumn=${key * valueFactor}}"""))
    withConfs(bucketingConf) {
      val df = catalogTable(table)
      shouldNotBeBucketed(ytScan(df))
      df.count() shouldBe keys.length
    }
  }

  it should "bucket a table hashed by a string column" in {
    withConfs(bucketingConf) {
      val df = catalogTable(stringKeyTable)
      df.schema(keyColumn).dataType shouldBe StringType
      keyGroupedPartitioning(ytScan(df)).keys().toSeq shouldEqual Seq(Expressions.bucket(buckets, keyColumn))
      val stored = readTableAsYson(stringKeyTable).map { node =>
        val row = node.asMap()
        (row.get(hashColumn).longValue(), row.get(keyColumn).stringValue().toLong, row.get(valueColumn).longValue())
      }
      shouldReadRowsIntoTheirBuckets(df, stored) { row =>
        (row.getAs[java.math.BigDecimal](hashColumn).longValueExact(), row.getAs[String](keyColumn).toLong,
          row.getAs[Long](valueColumn))
      }
    }
  }

  it should "not bucket a scan that bucketing does not apply to" in {
    val (noModuloTable, hashNotFirstTable) = (s"$tmpPath-no-modulo", s"$tmpPath-hash-not-first")
    val (unsortedTable, dynamicTable) = (s"$tmpPath-unsorted", s"$tmpPath-dynamic")
    awaitTables(
      startHashedTable(noModuloTable, s"farm_hash($keyColumn)", keyColumns, keyYsonRows),
      startHashedTable(hashNotFirstTable, expression, keyColumns, keyYsonRows, Seq(keyColumn, hashColumn)))
    writeKeyTable(unsortedTable, sorted = false)
    createDynamicHashedTable(dynamicTable, expression, keyColumns)
    val bucketed = () => catalogTable(bucketedTable)
    Seq[(String, Map[String, String], () => DataFrame)](
      ("hash bucketing disabled", Map(hashBucketingEnabledKey -> "false"), bucketed),
      ("Spark v2 bucketing disabled", Map(SQLConf.V2_BUCKETING_ENABLED.key -> "false"), bucketed),
      ("more buckets than the maximum", Map(maxBucketsKey -> (buckets - 1).toString), bucketed),
      ("distributed reading", Map(s"spark.yt.${Read.YtDistributedReadingEnabled.name}" -> "true"), bucketed),
      ("hash column read as bigint", Map(sessionOption(s"${hashColumn}_hint") -> LongType.json), bucketed),
      ("bigint hash argument read as string", Map(keyHint -> StringType.json), bucketed),
      ("string hash argument read as bigint", Map(keyHint -> LongType.json), () => catalogTable(stringKeyTable)),
      ("hash argument not read", Map.empty, () => catalogTable(bucketedTable).select(hashColumn, valueColumn)),
      ("read by path", Map.empty, () => spark.read.yt(bucketedTable)),
      ("hash column without modulo", Map.empty, () => catalogTable(noModuloTable)),
      ("first key column not the hash column", Map.empty, () => catalogTable(hashNotFirstTable)),
      ("unsorted table", Map.empty, () => catalogTable(unsortedTable)),
      ("dynamic table", Map.empty, () => catalogTable(dynamicTable))
    ).foreach { case (clue, conf, df) =>
      withConfs(bucketingConf ++ conf) {
        withClue(s"[$clue] ") {
          shouldNotBeBucketed(ytScan(df()))
        }
      }
    }
  }

  it should "not bucket a scan of several tables" in {
    withConfs(bucketingConf) {
      val df = spark.read.option(catalogReadOption, "true").yt(bucketedTable, oneBucketTable)
      shouldNotBeBucketed(ytScan(df))
      df.count() shouldBe 2 * keys.length
    }
  }

  it should "not bucket a scan partitioned by plan optimization key partitions" in {
    withConfs(bucketingConf) {
      val scan = ytScan(catalogTable(bucketedTable))
      keyGroupedPartitioning(scan)
      val keyPartitions = Seq(FilePartition(0, scan.getPartitions.flatMap(_.files).toArray))
      shouldNotBeBucketed(scan.copy(keyPartitionsHint = Some(keyPartitions)))
    }
  }

  it should "not bucket a table hashed by a uuid column, whose Spark text is not the hashed bytes" in {
    val table = s"$tmpPath-uuid-key"
    val uuidKeys = 1 to 4
    def uuidLiteral(key: Int): String = (0 until 16).map(offset => f"\\x${(key * 16 + offset) & 0xff}%02x").mkString
    writeHashedTable(
      table,
      expression,
      Seq(keyColumn -> TiType.uuid()),
      uuidKeys.map(key => s"""{$keyColumn="${uuidLiteral(key)}";$valueColumn=$key}"""))
    withConfs(bucketingConf) {
      val df = catalogTable(table)
      df.schema(keyColumn).dataType shouldBe StringType
      shouldNotBeBucketed(ytScan(df))
      df.collect().map(_.getAs[Long](valueColumn)) should contain theSameElementsAs uuidKeys.map(_.toLong)
    }
  }

  it should "not bucket a table hashed by a column whose Spark type the bucket function does not support" in {
    val table = s"$tmpPath-uint64-key"
    writeHashedTable(
      table,
      expression,
      Seq(keyColumn -> TiType.uint64()),
      keyRows.map { case (key, value) => s"{$keyColumn=${key}u;$valueColumn=$value}" })
    withConfs(bucketingConf) {
      val df = catalogTable(table)
      df.schema(keyColumn).dataType shouldBe DecimalType(20, 0)
      shouldNotBeBucketed(ytScan(df))
      df.collect().map(_.getAs[Long](valueColumn)) should contain theSameElementsAs keyRows.map(_._2)
    }
  }
}
