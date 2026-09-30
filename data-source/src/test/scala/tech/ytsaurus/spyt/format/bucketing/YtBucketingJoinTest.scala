package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTreeNode

class YtBucketingJoinTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir
  with BucketingJoinSuiteBase {
  behavior of "YtBucketingCatalog"

  private val buckets = 8

  private val keys = 1L to 24L
  private val leftValueFactor = 10L
  private val rightValueFactor = 100L

  private val leftTable = s"$tmpPath-left"
  private val rightTable = s"$tmpPath-right"
  private val plainTable = s"$tmpPath-plain"
  private val digitsTable = s"$tmpPath-digits"

  private val expectedRows = expectedJoinRows(keys, leftValueFactor, rightValueFactor)
  private val expectedExtendedRows = keys.map { key =>
    val extra = extraValue(key, leftExtraDivisor)
    val rightValue = if (extra == extraValue(key, rightExtraDivisor)) key * rightValueFactor else null
    Row(key, extra, key * leftValueFactor, rightValue)
  }

  private def plainLiteral(key: Any): String = key.toString

  private def uint64Literal(key: Any): String = s"${key}u"

  private def stringLiteral(key: Any): String = "\"" + key + "\""

  private def booleanLiteral(key: Any): String = if (key == true) "%true" else "%false"

  private def stringValue(node: YTreeNode): UTF8String = UTF8String.fromBytes(node.bytesValue())

  private def accentedKey(id: Long): String = if (id % 3 == 0) s"clé-$id" else s"key-$id"

  private val keyTypes = Seq(
    BucketKeyType("string", TiType.string(), StringType, accentedKey, stringLiteral, stringValue),
    BucketKeyType("utf8", TiType.utf8(), StringType, accentedKey, stringLiteral, stringValue),
    BucketKeyType(
      "json",
      TiType.json(),
      StringType,
      id => s"[$id]",
      stringLiteral,
      stringValue,
      keyColumnAllowed = false),
    BucketKeyType("int64", TiType.int64(), LongType, id => (id - 12) * 1000000007L, plainLiteral, _.longValue()),
    BucketKeyType("uint32", TiType.uint32(), LongType, id => id * 150000007L, uint64Literal, _.longValue()),
    BucketKeyType("uint16", TiType.uint16(), IntegerType, id => (id * 2700).toInt, uint64Literal, _.longValue().toInt),
    BucketKeyType("uint8", TiType.uint8(), ShortType, id => (id * 10).toShort, uint64Literal, _.longValue().toShort),
    BucketKeyType("interval", TiType.interval(), LongType, id => (id - 12) * 3600000000L, plainLiteral, _.longValue()),
    BucketKeyType(
      "int32",
      TiType.int32(),
      IntegerType,
      id => ((id - 12) * 100000007L).toInt,
      plainLiteral,
      _.longValue().toInt),
    BucketKeyType(
      "int16",
      TiType.int16(),
      ShortType,
      id => ((id - 12) * 2500).toShort,
      plainLiteral,
      _.longValue().toShort),
    BucketKeyType("int8", TiType.int8(), ByteType, id => ((id - 12) * 10).toByte, plainLiteral, _.longValue().toByte),
    BucketKeyType("bool", TiType.bool(), BooleanType, id => id % 2 == 0, booleanLiteral, _.boolValue()),
    BucketKeyType("date", TiType.date(), DateType, id => ((id - 1) * 997).toInt, uint64Literal, _.longValue().toInt),
    BucketKeyType(
      "timestamp",
      TiType.timestamp(),
      TimestampType,
      id => (id - 1) * 86400000000L + id,
      uint64Literal,
      _.longValue()))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tables = Seq(
      startBucketedKeyTable(leftTable, buckets, keys, leftValueFactor, leftExtraDivisor),
      startBucketedKeyTable(rightTable, buckets, keys, rightValueFactor, rightExtraDivisor),
      startHashedTable(
        digitsTable,
        s"farm_hash($keyColumn) % $buckets",
        Seq(keyColumn -> stringColumn),
        keys.map(key => s"""{$keyColumn="$key";$valueColumn=${key * leftValueFactor}}""")))
    awaitTables(tables ++ startTypedKeyTables(keyTypes): _*)
    writePlainTable(plainTable, rightValueFactor)
  }

  private def writePlainTable(path: String, valueFactor: Long): Unit = {
    val schema = TableSchema.builder().setUniqueKeys(false)
      .addValue(keyColumn, ColumnValueType.INT64)
      .addValue(valueColumn, ColumnValueType.INT64)
      .build()
    writeTableFromYson(keys.map(key => s"{$keyColumn=$key;$valueColumn=${key * valueFactor}}"), path, schema)
  }

  private def optimizerKeyGroupedPartitioning(df: DataFrame): Option[Seq[Expression]] = {
    df.queryExecution.optimizedPlan.collectFirst {
      case relation: DataSourceV2ScanRelation => relation.keyGroupedPartitioning
    }.flatten
  }

  private def extendedKeyJoin: DataFrame = {
    catalogJoin(leftTable, rightTable, Seq(keyColumn, extraColumn), "LEFT OUTER JOIN")
  }

  private val keyHint = sessionOption(s"${keyColumn}_hint")

  it should "join two farm_hash bucketed tables without a shuffle, with and without adaptive execution" in {
    Seq("false", "true").foreach { adaptive =>
      withConfs(joinConf + (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive)) {
        withClue(s"[adaptive $adaptive] ") {
          shouldJoinBucketed(shouldProduceReferenceRows(catalogJoin(leftTable, rightTable), expectedRows), buckets)
        }
      }
    }
  }

  it should "join on a superset of the hashed column without a shuffle when not all cluster keys are required" in {
    withConfs(joinConf ++ Map(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false")) {
      val query = shouldProduceReferenceRows(extendedKeyJoin, expectedExtendedRows)
      joins(query).length shouldBe 1
      shouldRunWithoutShuffles(query)
    }
  }

  it should "shuffle a join on a superset of the hashed column when all cluster keys are required" in {
    withConfs(joinConf) {
      spark.sessionState.conf.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION) shouldBe true
      shouldKeepShuffles(shouldProduceReferenceRows(extendedKeyJoin, expectedExtendedRows))
    }
  }

  it should "join a sparsely filled bucketed table without a shuffle" in {
    val sparseKeys = Seq(1L, 2L)
    val sparseTable = s"$tmpPath-sparse"
    awaitTables(startBucketedKeyTable(sparseTable, buckets, sparseKeys, leftValueFactor))
    withConfs(joinConf) {
      val query = shouldProduceReferenceRows(
        catalogJoin(sparseTable, rightTable),
        expectedJoinRows(sparseKeys, leftValueFactor, rightValueFactor))
      shouldJoinBucketed(query, buckets)
    }
  }

  it should "shuffle a join of tables with different bucket counts" in {
    val smallTable = s"$tmpPath-small"
    awaitTables(startBucketedKeyTable(smallTable, 4, keys, rightValueFactor, rightExtraDivisor))
    withConfs(joinConf) {
      shouldKeepShuffles(shouldProduceReferenceRows(catalogJoin(leftTable, smallTable), expectedRows))
    }
  }

  it should "bucket a catalog read only, so a join of tables read by path keeps its shuffles" in {
    withConfs(joinConf) {
      optimizerKeyGroupedPartitioning(catalogTable(leftTable)) shouldBe defined
      optimizerKeyGroupedPartitioning(spark.read.yt(leftTable)) shouldBe empty
      val left = spark.read.yt(leftTable).select(col(keyColumn), col(valueColumn).as(leftValueColumn))
      val right = spark.read.yt(rightTable).select(col(keyColumn), col(valueColumn).as(rightValueColumn))
      shouldKeepShuffles(shouldProduceReferenceRows(left.join(right, keyColumn), expectedRows))
    }
  }

  it should "keep the shuffle of a string hash argument retyped to bigint by a session schema hint" in {
    withConfs(joinConf + (keyHint -> LongType.json)) {
      catalogTable(digitsTable).schema(keyColumn).dataType shouldBe LongType
      optimizerKeyGroupedPartitioning(catalogTable(digitsTable)) shouldBe empty
      shouldKeepShuffles(shouldProduceReferenceRows(catalogJoin(digitsTable, rightTable), expectedRows))
    }
  }

  it should "keep every match of a string hash argument retyped to bigint when one-side shuffle is enabled" in {
    whenSparkVersionAtLeast(oneSideShuffleSparkMinVersion) {
      withConfs(joinConf ++ Map(oneSideShuffleKey -> "true", keyHint -> LongType.json)) {
        shouldProduceReferenceRows(catalogJoin(digitsTable, plainTable), expectedRows)
      }
    }
  }

  it should "shuffle only the side without a bucket column" in {
    whenSparkVersionAtLeast(oneSideShuffleSparkMinVersion) {
      withConfs(joinConf ++ Map(oneSideShuffleKey -> "true")) {
        shouldShuffleOnly(shouldProduceReferenceRows(catalogJoin(leftTable, plainTable), expectedRows), plainTable)
      }
    }
  }

  it should behave like typedKeyJoins(keyTypes)

  it should "not bucket a key column retyped by a schema hint, so one-side shuffle keeps every match" in {
    whenSparkVersionAtLeast(oneSideShuffleSparkMinVersion) {
      val digitsKeyType = BucketKeyType("digits", TiType.string(), StringType, _.toString, stringLiteral, stringValue)
      val digitsPlainTable = s"$tmpPath-digits-plain"
      writeTypedPlainKeyTable(digitsPlainTable, digitsKeyType, keys, rightValueFactor, withNullKey = true)
      withConfs(joinConf ++ Map(oneSideShuffleKey -> "true", keyHint -> StringType.json)) {
        catalogTable(leftTable).schema(keyColumn).dataType shouldBe StringType
        shouldMatchReferenceRows(catalogJoin(leftTable, digitsPlainTable), keys.length)
      }
    }
  }

  it should "load the table named by the identifier whatever path session option is set in any case" in {
    withConfs(joinConf + (sessionOption("PATH") -> rightTable)) {
      ytScan(catalogTable(leftTable)).options.get("path") shouldBe leftTable
    }
  }

  it should "list a directory with the session data source options like a read by path" in {
    val nestedDirectory = s"$tmpPath-nested"
    YtWrapper.createDir(s"$nestedDirectory/sub")
    writePlainTable(s"$nestedDirectory/sub/table", leftValueFactor)
    withConfs(joinConf + (sessionOption("recursiveFileLookup") -> "true")) {
      val byPath = spark.read.yt(nestedDirectory).collect().toSeq
      byPath should have length keys.length
      catalogTable(nestedDirectory).collect().toSeq should contain theSameElementsAs byPath
    }
  }

  it should "build scans with the session data source options like a read by path" in {
    val arrowOption = YtTableSparkSettings.ArrowEnabled.name
    withConfs(joinConf + (sessionOption(arrowOption) -> "false")) {
      ytScan(spark.read.yt(leftTable)).options.get(arrowOption) shouldBe "false"
      ytScan(catalogTable(leftTable)).options.get(arrowOption) shouldBe "false"
    }
  }

  it should "let a reader option override the session data source option of a catalog read" in {
    val readParallelismOption = "readParallelism"
    withConfs(joinConf + (sessionOption(readParallelismOption) -> "1")) {
      val df = spark.read.option(readParallelismOption, "3").table(catalogRelation(plainTable))
      ytScan(df).options.get(readParallelismOption) shouldBe "3"
    }
  }
}
