package org.apache.spark.sql.v2

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.expressions.{Expressions, FieldReference, LogicalExpressions, NamedReference}
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{EqualTo, Filter, In}
import org.apache.spark.sql.types.{DecimalType, LongType, StringType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.common.Decimal.textToBinary
import tech.ytsaurus.core.cypress.Range
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.format.YPathUtils.RichRangeCriteria
import tech.ytsaurus.spyt.format.YtPartitionedFileDelegate.YtPartitionedFile
import tech.ytsaurus.spyt.format.bucketing.HashBucketingTestUtils
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read
import tech.ytsaurus.spyt.format.conf.SparkYtInternalConfiguration
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.typeinfo.TiType

import java.util.Locale

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
  private val pushdownEnabledKey = s"spark.yt.${Read.KeyColumnsFilterPushdown.Enabled.name}"
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

  private def fileRange(file: PartitionedFile): Range = {
    val ranges = file.asInstanceOf[YtPartitionedFile].delegate.ypath.getRanges
    ranges should have size 1
    ranges.get(0).toRange
  }

  private def onlyRange(partition: FilePartition): Range = {
    partition.files should have length 1
    fileRange(partition.files.head)
  }

  private def shouldReadSingleKey(scan: YtScan, key: Long): Unit = {
    shouldKeyPartitionsByBucket(scan, buckets)
    scan.getPartitions.foreach { partition =>
      val range = onlyRange(partition)
      range.lower.key.get(0).longValue() shouldBe partition.index.toLong
      range.lower.key.get(1).longValue() shouldBe key
      range.upper.key.get(1).longValue() shouldBe key
    }
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
    withConfs(bucketingConf ++ Map(pushdownEnabledKey -> "true")) {
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

  it should "read matching rows when the second key is a YT decimal" in {
    val table = s"$tmpPath-decimal-key"
    val schema = TableSchema.builder()
      .addKeyExpression(hashColumn, ColumnValueType.UINT64, s"farm_hash($keyColumn) % 1")
      .addKey("amount", TiType.decimal(9, 0))
      .addValue(keyColumn, ColumnValueType.INT64)
      .build()
    val rows = Seq(7L, 9L).map { key =>
      new UnversionedRow(java.util.List.of(
        new UnversionedValue(0, ColumnValueType.UINT64, false, 0L),
        new UnversionedValue(1, ColumnValueType.STRING, false, textToBinary(key.toString, 9, 0)),
        new UnversionedValue(2, ColumnValueType.INT64, false, key)))
    }
    writeTableFromURow(rows, table, schema)

    def read(enabled: Boolean): Seq[Row] = {
      val conf = bucketingConf ++ Map(pushdownEnabledKey -> enabled.toString, s"spark.yt.${Read.TypeV3.name}" -> "true")
      withConfs(conf) {
        val df = catalogTable(table).filter(col("amount") === new java.math.BigDecimal("7")).select(keyColumn)
        shouldKeyPartitionsByBucket(ytScan(df), 1)
        df.collect().toSeq
      }
    }

    val expected = read(enabled = false)
    expected shouldEqual Seq(Row(7L))
    read(enabled = true) shouldEqual expected
  }

  it should "rebuild bucket ranges for changed and cleared runtime filters" in {
    val conf = bucketingConf ++ Map(
      pushdownEnabledKey -> "true",
      s"spark.yt.${Read.KeyColumnsFilterPushdown.MergeAdjacentEnabled.name}" -> "false")
    withConfs(conf) {
      val scan = ytScan(catalogTable(bucketedTable).select(keyColumn, valueColumn))
      scan.getPartitions.length shouldBe buckets
      scan.filter(Array[Filter](EqualTo(keyColumn, 7L)))
      shouldReadSingleKey(scan, 7L)
      scan.filter(Array[Filter](In(keyColumn, Array[Any](2L, 7L, 13L))))
      shouldKeyPartitionsByBucket(scan, buckets)
      scan.getPartitions.foreach { partition =>
        val lowerKeys = partition.files.toSeq.map { file =>
          fileRange(file).lower.key.get(1).longValue()
        }
        lowerKeys shouldEqual Seq(2L, 7L, 13L)
      }
      scan.filter(Array.empty[Filter])
      scan.getPartitions.foreach { partition =>
        onlyRange(partition).lower.key.size() shouldBe 1
      }
      val df = catalogTable(bucketedTable).filter(col(keyColumn).isin(2L, 7L, 13L))
      keyValueRows(df) should contain theSameElementsAs keyRows.filter(row => Set(2L, 7L, 13L).contains(row._1))
    }
  }

  it should "read only the requested interval of the second key in each bucket" in {
    withConfs(bucketingConf ++ Map(pushdownEnabledKey -> "true")) {
      val df = catalogTable(bucketedTable).filter(col(keyColumn) >= 5L && col(keyColumn) <= 13L)
      val scan = ytScan(df)
      shouldKeyPartitionsByBucket(scan, buckets)
      scan.getPartitions.foreach { partition =>
        val range = onlyRange(partition)
        range.lower.key.get(1).longValue() shouldBe 5L
        range.upper.key.get(1).longValue() shouldBe 13L
      }
      keyValueRows(df) should contain theSameElementsAs keyRows.filter { case (key, _) => key >= 5L && key <= 13L }
    }
  }

  it should "narrow bucket ranges and reduce scan output rows for a static key filter" in {
    def scannedRows(enabled: Boolean): Long = {
      withConfs(bucketingConf ++ Map(pushdownEnabledKey -> enabled.toString)) {
        val df = catalogTable(bucketedTable).filter(col(keyColumn) === 7L).select(keyColumn, valueColumn)
        if (enabled) {
          shouldReadSingleKey(ytScan(df), 7L)
        }
        keyValueRows(df) should contain theSameElementsAs keyRows.filter(_._1 == 7L)
        df.queryExecution.executedPlan.collectFirst {
          case scan: BatchScanExec => scan.metrics("numOutputRows").value
        }.get
      }
    }

    val withoutPushdown = scannedRows(enabled = false)
    val withPushdown = scannedRows(enabled = true)
    withoutPushdown shouldBe keys.length.toLong
    withPushdown should be < withoutPushdown
  }

  it should "limit bucket ranges without discarding values" in {
    val conf = bucketingConf ++ Map(
      pushdownEnabledKey -> "true",
      s"spark.yt.${Read.KeyColumnsFilterPushdown.MergeAdjacentEnabled.name}" -> "false",
      s"spark.yt.${Read.KeyColumnsFilterPushdown.YtPathCountLimit.name}" -> "2")
    withConfs(conf) {
      val df = catalogTable(bucketedTable).filter(col(keyColumn).isin(2L, 7L, 13L))
      val scan = ytScan(df)
      shouldKeyPartitionsByBucket(scan, buckets)
      scan.getPartitions.foreach { partition =>
        val range = onlyRange(partition)
        range.lower.key.get(1).longValue() shouldBe 2L
        range.upper.key.get(1).longValue() shouldBe 13L
      }
      keyValueRows(df) should contain theSameElementsAs keyRows.filter(row => Set(2L, 7L, 13L).contains(row._1))
    }
  }

  it should "keep keyed empty partitions for contradictory static and runtime filters" in {
    withConfs(bucketingConf ++ Map(pushdownEnabledKey -> "true")) {
      val scan = ytScan(catalogTable(bucketedTable).filter(col(keyColumn) === 7L).select(keyColumn, valueColumn))
      scan.filter(Array[Filter](EqualTo(keyColumn, 13L)))
      shouldKeyPartitionsByBucket(scan, buckets)
      all(scan.getPartitions.map(_.files.length)) shouldBe 0
    }
  }

  it should "narrow bucket ranges with a dynamic pruning filter in a join" in {
    val conf = bucketingConf ++ Map(
      pushdownEnabledKey -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true")
    withConfs(conf) {
      val fact = catalogTable(bucketedTable).select(keyColumn, valueColumn)
      val dimension = spark.range(1L, 25L).selectExpr("id AS dim_key", "id % 3 AS dim_group")
        .filter(col("dim_group") === 1L)
      val query = fact.join(broadcast(dimension), fact(keyColumn) === dimension("dim_key"))
        .select(fact(keyColumn), fact(valueColumn))

      query.queryExecution.executedPlan.toString.toLowerCase(Locale.ROOT) should include("dynamicpruning")
      val expected = keyRows.filter(_._1 % 3L == 1L)
      val expectedBounds = expected.map { case (key, _) => (key, key) }
      keyValueRows(query) should contain theSameElementsAs expected
      val scan = ytScan(query)
      shouldKeyPartitionsByBucket(scan, buckets)
      scan.getPartitions.foreach { partition =>
        val bounds = partition.files.toSeq.map { file =>
          val range = fileRange(file)
          (range.lower.key.get(1).longValue(), range.upper.key.get(1).longValue())
        }
        bounds should contain theSameElementsAs expectedBounds
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
