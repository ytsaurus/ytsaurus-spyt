package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.v2.YtScan
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.bucketing.HashBucketingTestUtils.PendingTable
import tech.ytsaurus.spyt.format.bucketing.YtBucketingCatalog.BoundBucketFunction
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.PlanOptimizationEnabled
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTreeNode

/** A bucket key type of test tables; a type YTsaurus refuses in key columns is stored as a value column. */
case class BucketKeyType(
  name: String,
  tiType: TiType,
  sparkType: DataType,
  key: Long => Any,
  ysonLiteral: Any => String,
  internalValue: YTreeNode => Any,
  keyColumnAllowed: Boolean = true)

trait BucketingJoinSuiteBase extends HashBucketingTestUtils {
  self: AnyFlatSpec with Matchers with LocalSpark with TmpDir =>

  protected val leftValueColumn = "lv"
  protected val rightValueColumn = "rv"

  protected val planOptimizationKey = s"spark.yt.${PlanOptimizationEnabled.name}"

  protected val oneSideShuffleKey = "spark.sql.sources.v2.bucketing.shuffle.enabled"
  protected val oneSideShuffleSparkMinVersion = "4.0.0"

  protected val joinConf: Map[String, String] = bucketingConf ++ Map(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    planOptimizationKey -> "false")

  private val typedKeyBuckets = 10
  private val typedKeyIds = 1L to 24L
  private val typedLeftValueFactor = 10L
  private val typedRightValueFactor = 100L

  protected def typedTable(keyType: BucketKeyType, role: String): String = s"$tmpPath-${keyType.name}-$role"

  /** Starts the left table of each key type, with a NULL key, and the right one, without it, for typedKeyJoins. */
  protected def startTypedKeyTables(keyTypes: Seq[BucketKeyType]): Seq[PendingTable] = {
    keyTypes.flatMap { keyType =>
      Seq(
        startTypedBucketedKeyTable(typedTable(keyType, "left"), keyType, typedLeftValueFactor, withNullKey = true),
        startTypedBucketedKeyTable(typedTable(keyType, "right"), keyType, typedRightValueFactor, withNullKey = false))
    }
  }

  /** Join scenarios shared by every bucket key type, over the tables of startTypedKeyTables. */
  protected def typedKeyJoins(keyTypes: => Seq[BucketKeyType]): Unit = {
    it should "join tables bucketed by a key of each tested type without a shuffle" in {
      withConfs(joinConf) {
        keyTypes.foreach { keyType =>
          withClue(s"[${keyType.name}] ") {
            val (left, right) = (typedTable(keyType, "left"), typedTable(keyType, "right"))
            val query = shouldMatchReferenceRows(catalogJoin(left, right), innerJoinRowCount(keyType))
            shouldJoinBucketed(query, typedKeyBuckets)
          }
        }
      }
    }

    it should "shuffle only the side without a bucket column for a key of each tested type" in {
      whenSparkVersionAtLeast(oneSideShuffleSparkMinVersion) {
        withConfs(joinConf ++ Map(oneSideShuffleKey -> "true")) {
          keyTypes.foreach { keyType =>
            withClue(s"[${keyType.name}] ") {
              val (bucketed, plain) = (typedTable(keyType, "left"), typedTable(keyType, "plain"))
              writeTypedPlainKeyTable(plain, keyType, typedKeyIds, typedRightValueFactor, withNullKey = true)
              val innerJoin = shouldMatchReferenceRows(catalogJoin(bucketed, plain), innerJoinRowCount(keyType))
              shouldShuffleOnly(innerJoin, plain)
              val outerJoin = shouldMatchReferenceRows(
                catalogJoin(plain, bucketed, joinType = "LEFT OUTER JOIN"),
                innerJoinRowCount(keyType) + 1)
              shouldShuffleOnly(outerJoin, plain)
            }
          }
        }
      }
    }

    it should "compute the bucket that YTsaurus stored for a key of each tested type, null included" in {
      keyTypes.foreach { keyType =>
        val stored = storedBuckets(typedTable(keyType, "left"), keyType)
        stored.count(_._1 == null) shouldBe 1
        stored.foreach { case (key, hash) =>
          withClue(s"[${keyType.name}] bucket of $key: ") {
            val row = new GenericInternalRow(Array[Any](typedKeyBuckets, key))
            BoundBucketFunction(keyType.sparkType).produceResult(row).longValue() shouldEqual hash
          }
        }
      }
    }
  }

  protected def writeTypedPlainKeyTable(
    path: String,
    keyType: BucketKeyType,
    ids: Seq[Long],
    valueFactor: Long,
    withNullKey: Boolean): Unit = {
    val schema = TableSchema.builder().setUniqueKeys(false)
      .addValue(keyColumn, keyColumnType(keyType, withNullKey))
      .addValue(valueColumn, ColumnValueType.INT64)
      .build()
    writeTableFromYson(typedKeyRows(keyType, ids, valueFactor, withNullKey), path, schema)
  }

  protected def storedBuckets(table: String, keyType: BucketKeyType): Seq[(Any, Long)] = {
    readTableAsYson(table).map { node =>
      val row = node.asMap()
      val key = row.get(keyColumn)
      (if (key.isEntityNode) null else keyType.internalValue(key), row.get(hashColumn).longValue())
    }
  }

  protected def expectedJoinRows(keys: Seq[Long], leftValueFactor: Long, rightValueFactor: Long): Seq[Row] = {
    keys.map(key => Row(key, key * leftValueFactor, key * rightValueFactor))
  }

  protected def joins(query: DataFrame): Seq[SparkPlan] = {
    AdaptivePlans.collect(query.queryExecution.executedPlan) { case node: BaseJoinExec => node }
  }

  protected def shouldRunWithoutShuffles(query: DataFrame): Unit = {
    val exchanges = shuffleExchanges(query) ++ reusedExchanges(query)
    withClue(s"expected no shuffle exchanges, found ${exchanges.map(_.nodeName).mkString("[", ", ", "]")}: ") {
      exchanges shouldBe empty
    }
  }

  /** Checks that the query is a single join of two scans of `buckets` partitions each, run without shuffles. */
  protected def shouldJoinBucketed(query: DataFrame, buckets: Int): Unit = {
    joins(query).length shouldBe 1
    shouldRunWithoutShuffles(query)
    AdaptivePlans.collect(query.queryExecution.executedPlan) {
      case scan: BatchScanExec => scan.inputRDD.getNumPartitions
    } shouldEqual Seq(buckets, buckets)
  }

  protected def shouldKeepShuffles(query: DataFrame): Unit = {
    withClue("expected the join to keep its shuffles: ") {
      shuffleExchanges(query) should not be empty
    }
  }

  protected def shouldShuffleOnly(query: DataFrame, table: String): Unit = {
    val shuffles = shuffleExchanges(query)
    withClue(s"expected exactly one shuffle, found ${shuffles.map(_.nodeName).mkString("[", ", ", "]")}: ") {
      shuffles.length shouldBe 1
    }
    val shuffledPaths = AdaptivePlans.collect(shuffles.head) { case scan: BatchScanExec => scan.scan }.map {
      case scan: YtScan => scan.options.get("path")
      case scan => scan.description()
    }
    withClue(s"expected the shuffled side to read only $table: ") {
      shuffledPaths should contain only table
    }
  }

  protected def catalogJoin(
    left: String,
    right: String,
    joinColumns: Seq[String] = Seq(keyColumn),
    joinType: String = "JOIN"): DataFrame = {
    val selected = joinColumns.map(column => s"a.$column AS $column") ++
      Seq(s"a.$valueColumn AS $leftValueColumn", s"b.$valueColumn AS $rightValueColumn")
    val condition = joinColumns.map(column => s"a.$column = b.$column").mkString(" AND ")
    spark.sql(s"SELECT ${selected.mkString(", ")} FROM ${catalogRelation(left)} a " +
      s"$joinType ${catalogRelation(right)} b ON $condition")
  }

  /**
   * Collects the query and compares its rows, with their multiplicity, with the same query planned without hash
   * bucketing. Returns the executed DataFrame, so that plan checks inspect the very execution whose rows matched.
   */
  protected def shouldProduceReferenceRows(query: => DataFrame, expected: Seq[Row]): DataFrame = {
    shouldMatchReference(query)(_ should contain theSameElementsAs expected)
  }

  protected def shouldMatchReferenceRows(query: => DataFrame, expectedRowCount: Int): DataFrame = {
    shouldMatchReference(query)(_ should have length expectedRowCount)
  }

  private def shouldMatchReference(query: => DataFrame)(checkReference: Seq[Row] => Unit): DataFrame = {
    val reference = withoutHashBucketing(query.collect().toSeq)
    checkReference(reference)
    val executed = query
    executed.collect().toSeq should contain theSameElementsAs reference
    executed
  }

  private def withoutHashBucketing[R](f: => R): R = withConf(hashBucketingEnabledKey, "false")(f)

  private def startTypedBucketedKeyTable(
    path: String,
    keyType: BucketKeyType,
    valueFactor: Long,
    withNullKey: Boolean): PendingTable = {
    startHashedTable(
      path,
      s"farm_hash($keyColumn) % $typedKeyBuckets",
      Seq(keyColumn -> keyColumnType(keyType, withNullKey)),
      typedKeyRows(keyType, typedKeyIds, valueFactor, withNullKey),
      if (keyType.keyColumnAllowed) Nil else Seq(hashColumn))
  }

  /**
   * The inner self-join size of the typed keys. Generators that map several ids to one key, such as the boolean one,
   * are intentional: each such key multiplies join rows, which the row comparisons count.
   */
  private def innerJoinRowCount(keyType: BucketKeyType): Int = {
    typedKeyIds.groupBy(keyType.key).values.map(group => group.size * group.size).sum
  }

  private def typedKeyRows(
    keyType: BucketKeyType,
    ids: Seq[Long],
    valueFactor: Long,
    withNullKey: Boolean): Seq[String] = {
    val rows = ids.map(id => s"{$keyColumn=${keyType.ysonLiteral(keyType.key(id))};$valueColumn=${id * valueFactor}}")
    if (withNullKey) rows :+ s"{$keyColumn=#;$valueColumn=0}" else rows
  }

  private def keyColumnType(keyType: BucketKeyType, withNullKey: Boolean): TiType = {
    if (withNullKey) TiType.optional(keyType.tiType) else keyType.tiType
  }

  /** Shuffles the query runs; a reused exchange reads the output of one of them and is counted apart. */
  private def shuffleExchanges(query: DataFrame): Seq[SparkPlan] = {
    AdaptivePlans.collect(query.queryExecution.executedPlan) { case exchange: ShuffleExchangeExec => exchange }
  }

  private def reusedExchanges(query: DataFrame): Seq[SparkPlan] = {
    AdaptivePlans.collect(query.queryExecution.executedPlan) { case exchange: ReusedExchangeExec => exchange }
  }
}

private[bucketing] object AdaptivePlans extends AdaptiveSparkPlanHelper
