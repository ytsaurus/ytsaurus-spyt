package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.format.bucketing.YtBucketingCatalog.BoundBucketFunction
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.KeyPartitioning
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.typeinfo.TiType

/** Bucketing specifics of extended types, where a uint64 column, the hash column included, is read as UInt64Type. */
class ExtendedYtBucketingJoinTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir
  with BucketingJoinSuiteBase {
  behavior of "YtBucketingCatalog"

  private val buckets = 8
  private val keys = 1L to 24L
  private val leftValueFactor = 10L
  private val rightValueFactor = 100L

  private val leftTable = s"$tmpPath-left"
  private val rightTable = s"$tmpPath-right"

  private val expectedRows = expectedJoinRows(keys, leftValueFactor, rightValueFactor)

  private val uint64KeyType = BucketKeyType(
    "uint64",
    TiType.uint64(),
    UInt64Type,
    id => if (id <= 16) id else if (id <= 20) Long.MinValue + id else -id,
    key => java.lang.Long.toUnsignedString(key.asInstanceOf[Long]) + "u",
    _.longValue())

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tables = Seq(
      startBucketedKeyTable(leftTable, buckets, keys, leftValueFactor),
      startBucketedKeyTable(rightTable, buckets, keys, rightValueFactor, rightExtraDivisor))
    awaitTables(tables ++ startTypedKeyTables(Seq(uint64KeyType)): _*)
  }

  it should "join two farm_hash bucketed tables with a UInt64Type hash column without a shuffle" in {
    withConfs(joinConf) {
      catalogTable(leftTable).schema(hashColumn).dataType shouldBe UInt64Type
      shouldJoinBucketed(shouldProduceReferenceRows(catalogJoin(leftTable, rightTable), expectedRows), buckets)
    }
  }

  it should "join two farm_hash bucketed tables without a shuffle when plan optimization is enabled" in {
    val planOptimizationConf = Map(planOptimizationKey -> "true", s"spark.yt.${KeyPartitioning.Enabled.name}" -> "true")
    withConfs(joinConf ++ planOptimizationConf) {
      shouldJoinBucketed(shouldProduceReferenceRows(catalogJoin(leftTable, rightTable), expectedRows), buckets)
    }
  }

  it should "bind the bucket function to a UInt64Type argument" in {
    val bound = YtBucketingCatalog.BucketFunction
      .bind(StructType(Seq(StructField("buckets", IntegerType), StructField(keyColumn, UInt64Type))))
    bound shouldBe BoundBucketFunction(UInt64Type)
    bound.canonicalName() shouldBe "spyt.farm_hash_bucket(int, uint64)"
  }

  it should "read a uint64 key as UInt64Type, keys above 2^63 included" in {
    val table = typedTable(uint64KeyType, "left")
    catalogTable(table).schema(keyColumn).dataType shouldBe UInt64Type
    val stored = storedBuckets(table, uint64KeyType)
    stored.count { case (key, _) => key != null && key.asInstanceOf[Long] < 0 } should be > 0
  }

  it should behave like typedKeyJoins(Seq(uint64KeyType))
}
