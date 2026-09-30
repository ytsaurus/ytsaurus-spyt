package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.v2.{Utils, YtDataSourceV2, YtKeyedFilePartition, YtScan}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.client.request.{GetOperation, StartOperation}
import tech.ytsaurus.core.GUID
import tech.ytsaurus.core.cypress.CypressNodeType
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.rpcproxy.EOperationType
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.HashBucketing
import tech.ytsaurus.spyt.test.{LocalSpark, TestTableSettings, TestUtils}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait HashBucketingTestUtils extends TestUtils {
  self: TestSuite with Matchers with LocalSpark =>

  import HashBucketingTestUtils.{PendingTable, operationTimeout}

  protected val hashColumn = "hash"
  protected val keyColumn = "k"
  protected val extraColumn = "d"
  protected val valueColumn = "v"

  protected val int64Column: TiType = TiType.optional(TiType.int64())
  protected val stringColumn: TiType = TiType.optional(TiType.string())

  protected val hashBucketingEnabledKey = s"spark.yt.${HashBucketing.Enabled.name}"
  private val catalogName: String = YtBucketingCatalog.defaultCatalogName

  protected val bucketingConf: Map[String, String] = Map(
    hashBucketingEnabledKey -> "true",
    SQLConf.V2_BUCKETING_ENABLED.key -> "true")

  protected val leftExtraDivisor = 3L
  protected val rightExtraDivisor = 2L

  private val partitionIdColumn = "partition_id"
  private val sessionOptionPrefix = s"spark.datasource.${new YtDataSourceV2().keyPrefix()}."
  private val finishedOperationStates = Set("completed", "failed", "aborted")

  protected def extraValue(key: Long, extraDivisor: Long): Long = key % extraDivisor

  protected def startBucketedKeyTable(
    path: String,
    buckets: Int,
    keys: Seq[Long],
    valueFactor: Long,
    extraDivisor: Long = leftExtraDivisor): PendingTable = {
    val rows = keys.map { key =>
      s"{$keyColumn=$key;$extraColumn=${extraValue(key, extraDivisor)};$valueColumn=${key * valueFactor}}"
    }
    startHashedTable(
      path,
      s"farm_hash($keyColumn) % $buckets",
      Seq(keyColumn -> int64Column, extraColumn -> int64Column),
      rows)
  }

  /**
   * Starts sorting `rows` into a table keyed by `sortBy`, by default the hash column followed by `columns`. The rows
   * are written unsorted to a separate source table and sorted by YTsaurus, which computes the hash column and orders
   * the rows by it: the tests then compare the Spark bucket with the real YTsaurus farm_hash, not with the JVM
   * implementation under test. Bucketed tables are created the same way in practice; the sort cannot run in place since
   * the source table has no hash column.
   */
  protected def startHashedTable(
    path: String,
    expression: String,
    columns: Seq[(String, TiType)],
    rows: Seq[String],
    sortBy: Seq[String] = Nil,
    optimizeForScan: Boolean = false): PendingTable = {
    val keys = if (sortBy.isEmpty) defaultSortBy(columns) else sortBy
    val source = s"$path-source"
    val sourceSchema = columns.foldLeft(TableSchema.builder().setUniqueKeys(false)) {
      case (builder, (name, columnType)) => builder.addValue(name, columnType)
    }.addValue(valueColumn, ColumnValueType.INT64).build()
    writeTableFromYson(rows, source, sourceSchema)
    val attributes = Map[String, YTreeNode]("schema" -> hashedSchema(expression, columns, keys).toYTree) ++
      (if (optimizeForScan) Map("optimize_for" -> YTree.stringNode("scan")) else Map.empty)
    yt.createNode(path, CypressNodeType.TABLE, attributes.asJava).join()
    PendingTable(path, startSort(source, path, keys))
  }

  /** Waits for every sort under one deadline and fails on each operation that did not complete. */
  protected def awaitTables(tables: PendingTable*): Unit = {
    val deadline = operationTimeout.fromNow
    val notCompleted = tables.map(table => table -> awaitOperation(table.operation, deadline))
      .filter { case (_, attributes) => attributes.get("state").stringValue() != "completed" }
    if (notCompleted.nonEmpty) {
      fail("Sort operations did not complete: " + notCompleted.map { case (table, attributes) =>
        s"${table.path} (operation ${table.operation}): ${attributes.get("state").stringValue()}, " +
          s"result: ${attributes.get("result")}"
      }.mkString("; "))
    }
  }

  protected def createEmptyHashedTable(path: String, expression: String, columns: Seq[(String, TiType)]): Unit = {
    createEmptyTable(path, hashedSchema(expression, columns, defaultSortBy(columns)))
  }

  protected def createDynamicHashedTable(path: String, expression: String, columns: Seq[(String, TiType)]): Unit = {
    val keys = defaultSortBy(columns)
    val schema = hashedSchema(expression, columns, keys)
    YtWrapper.createTable(path, TestTableSettings(schema.toYTree, isDynamic = true, keys.asJava))
    YtWrapper.mountTableSync(path)
  }

  protected def sessionOption(name: String): String = s"$sessionOptionPrefix$name"

  protected def catalogRelation(path: String): String = s"$catalogName.`$path`"

  protected def catalogTable(path: String): DataFrame = spark.sql(s"SELECT * FROM ${catalogRelation(path)}")

  protected def ytScan(df: DataFrame): YtScan = Utils.extractYtScan(df.queryExecution.executedPlan)

  protected def keyGroupedPartitioning(scan: YtScan): KeyGroupedPartitioning = {
    val partitioning = scan.outputPartitioning()
    partitioning shouldBe a[KeyGroupedPartitioning]
    partitioning.asInstanceOf[KeyGroupedPartitioning]
  }

  protected def shouldKeyPartitionsByBucket(scan: YtScan, buckets: Int): Unit = {
    val partitionKeys = scan.getPartitions.map {
      case partition: YtKeyedFilePartition => (partition.index, partition.partitionKey().getLong(0))
      case partition => fail(s"Expected YtKeyedFilePartition, got ${partition.getClass.getSimpleName}")
    }
    partitionKeys shouldEqual (0 until buckets).map(bucket => (bucket, bucket.toLong))
  }

  /** Rows of a table as (hash, key, value). */
  protected def storedRows(table: String): Seq[(Long, Long, Long)] = {
    readTableAsYson(table).map { node =>
      val row = node.asMap()
      (row.get(hashColumn).longValue(), row.get(keyColumn).longValue(), row.get(valueColumn).longValue())
    }
  }

  /** Checks that `df` reads exactly the `expected` (hash, key, value) rows, each in the partition of its hash. */
  protected def shouldReadRowsIntoTheirBuckets(df: DataFrame, expected: Seq[(Long, Long, Long)])
    (scanRow: Row => (Long, Long, Long)): Unit = {
    val rows = df.withColumn(partitionIdColumn, spark_partition_id()).collect().toSeq
    rows.map(row => (row.getAs[Int](partitionIdColumn).toLong, scanRow(row))) should contain theSameElementsAs
      expected.map(row => (row._1, row))
  }

  private def defaultSortBy(columns: Seq[(String, TiType)]): Seq[String] = hashColumn +: columns.map(_._1)

  private def hashedSchema(expression: String, columns: Seq[(String, TiType)], sortBy: Seq[String]): TableSchema = {
    val columnTypes = columns.toMap
    val withKeys = sortBy.foldLeft(TableSchema.builder().setUniqueKeys(false)) { (builder, key) =>
      if (key == hashColumn) {
        builder.addKeyExpression(key, ColumnValueType.UINT64, expression)
      } else {
        builder.addKey(key, columnTypes.getOrElse(key, fail(s"No column $key to sort by")))
      }
    }
    columns.filterNot { case (name, _) => sortBy.contains(name) }
      .foldLeft(withKeys) { case (builder, (name, columnType)) => builder.addValue(name, columnType) }
      .addValue(valueColumn, ColumnValueType.INT64).build()
  }

  private def startSort(source: String, destination: String, sortBy: Seq[String]): GUID = {
    val spec = YTree.builder()
      .beginMap()
      .key("input_table_paths").value(Seq(source).asJava)
      .key("output_table_path").value(destination)
      .key("sort_by").value(sortBy.asJava)
      .endMap()
      .build()
    yt.startOperation(new StartOperation(EOperationType.OT_SORT, spec)).join()
  }

  // Polls instead of Operation.watch(), which checks the state only once per client ping period of 30 seconds.
  @tailrec
  private def awaitOperation(operation: GUID, deadline: Deadline): java.util.Map[String, YTreeNode] = {
    val attributes = yt.getOperation(new GetOperation(operation)).join().asMap()
    if (finishedOperationStates.contains(attributes.get("state").stringValue())) {
      attributes
    } else if (deadline.isOverdue()) {
      fail(s"Sort operation $operation did not finish within $operationTimeout")
    } else {
      Thread.sleep(200)
      awaitOperation(operation, deadline)
    }
  }
}

object HashBucketingTestUtils {
  private val operationTimeout = 2.minutes

  /** A test table whose sort operation has started, see [[HashBucketingTestUtils#awaitTables]]. */
  case class PendingTable(path: String, operation: GUID)
}
