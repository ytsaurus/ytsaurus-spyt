package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Literal, V2ExpressionUtils}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.spyt.types.DatetimeType
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.format.bucketing.YtBucketingCatalog.{BoundBucketFunction, bucketFunctionName}

import java.util.Collections

class YtBucketingFunctionTest extends AnyFlatSpec with Matchers {
  behavior of "YtBucketingCatalog"

  private val buckets = 10

  // farm_hash(NULL) % 10; YTsaurus hashes NULL as the int64 0
  private val bucketOfNull = 1L

  // farm_hash(42) % 10
  private val bucketOf42 = 4L

  // farm_hash(1) % 10
  private val bucketOf1 = 9L

  // farm_hash('key') % 10
  private val bucketOfKey = 6L

  private val bucketIdentifier = Identifier.of(Array.empty, bucketFunctionName)

  private def catalog: YtBucketingCatalog = {
    val catalog = new YtBucketingCatalog()
    catalog.initialize(YtBucketingCatalog.defaultCatalogName, CaseInsensitiveStringMap.empty())
    catalog
  }

  private def bindBucket(fields: StructField*) = catalog.loadFunction(bucketIdentifier).bind(StructType(fields))

  private def resolve(bucketCount: Expression, argument: Expression, argumentType: DataType = LongType): Expression = {
    V2ExpressionUtils.resolveScalarFunction(BoundBucketFunction(argumentType), Seq(bucketCount, argument))
  }

  it should "put a NULL argument into the bucket of 0, as YTsaurus farm_hash does, not the previous value" in {
    Seq[(DataType, Any, Long)](
      (LongType, 42L, bucketOf42),
      (StringType, UTF8String.fromString("key"), bucketOfKey)).foreach { case (argumentType, value, bucket) =>
      withClue(s"[$argumentType]: ") {
        val call = resolve(Literal(buckets), BoundReference(0, argumentType, nullable = true), argumentType)
        call.nullable shouldBe false
        // ApplyFunctionExpression passes every row in one reused SpecificInternalRow, whose setNullAt keeps the
        // previous value in the slot: a NULL after a value must not get its bucket, a value after a NULL is hashed.
        Seq(value, null, value).map(argument => call.eval(InternalRow(argument))) shouldEqual
          Seq(bucket, bucketOfNull, bucket)
      }
    }
  }

  it should "hash a NULL argument to the uint64 3315701238936582721 that YTsaurus farm_hash gives NULL" in {
    // Remainders modulo three pairwise coprime counts whose product exceeds 2^64 determine the whole hash.
    Seq(2147483647 -> 2083173292L, 2147483646 -> 1479683353L, 2147483645 -> 876193416L).foreach {
      case (bucketCount, bucket) =>
        resolve(Literal(bucketCount), Literal(null, LongType)).eval(InternalRow.empty) shouldEqual bucket
    }
  }

  it should "reject a bucket count that is not positive" in {
    Seq(0, -1, Int.MinValue).foreach { bucketCount =>
      withClue(s"[$bucketCount]: ") {
        val call = resolve(Literal(bucketCount), Literal(1L))
        val error = the[IllegalArgumentException] thrownBy call.eval(InternalRow.empty)
        error.getMessage shouldBe s"bucket count must be positive, got $bucketCount"
      }
    }
  }

  it should "reject a NULL bucket count instead of reusing the previous one" in {
    val call = resolve(BoundReference(0, IntegerType, nullable = true), Literal(1L))
    call.eval(InternalRow(buckets)) shouldEqual bucketOf1
    an[IllegalArgumentException] should be thrownBy call.eval(InternalRow(null))
  }

  it should "bind the bucket function to an int bucket count and each supported argument type" in {
    Seq(LongType, IntegerType, ShortType, ByteType, BooleanType, StringType, DateType, TimestampType).foreach {
      argumentType =>
        withClue(s"[$argumentType]: ") {
          val bound = bindBucket(StructField("buckets", IntegerType), StructField("k", argumentType))
          bound shouldBe BoundBucketFunction(argumentType)
          bound.canonicalName() shouldBe s"spyt.farm_hash_bucket(int, ${argumentType.catalogString})"
          bound.resultType() shouldBe LongType
          bound.inputTypes().toSeq shouldEqual Seq(IntegerType, argumentType)
          bound.isResultNullable shouldBe false
        }
    }
  }

  it should "reject bucket function arguments of unsupported types or shapes" in {
    Seq(DoubleType, FloatType, DecimalType(20, 0), BinaryType, new DatetimeType()).foreach { argumentType =>
      withClue(s"[$argumentType]: ") {
        an[UnsupportedOperationException] should be thrownBy
          bindBucket(StructField("buckets", IntegerType), StructField("k", argumentType))
      }
    }
    an[UnsupportedOperationException] should be thrownBy
      bindBucket(StructField("buckets", LongType), StructField("k", LongType))
    an[UnsupportedOperationException] should be thrownBy
      bindBucket(StructField("buckets", IntegerType), StructField("k", LongType), StructField("d", LongType))
  }

  it should "refuse the catalog name yt, which SQL reads by path already use, and a missing name" in {
    Seq("yt", "YT", "", null).foreach { name =>
      an[IllegalArgumentException] should be thrownBy
        new YtBucketingCatalog().initialize(name, CaseInsensitiveStringMap.empty())
    }
  }

  it should "accept any other catalog name and report it only after initialization" in {
    an[IllegalStateException] should be thrownBy new YtBucketingCatalog().name()
    Seq("ytsaurus", "YTSAURUS", "buckets").foreach { name =>
      val named = new YtBucketingCatalog()
      named.initialize(name, CaseInsensitiveStringMap.empty())
      named.name() shouldBe name
    }
  }

  it should "expose only the bucket function and load tables from the empty namespace only" in {
    catalog.listFunctions(Array.empty).toSeq shouldEqual Seq(bucketIdentifier)
    catalog.listFunctions(Array("ns")) shouldBe empty
    a[NoSuchFunctionException] should be thrownBy catalog.loadFunction(Identifier.of(Array.empty, "farm_hash"))
    a[NoSuchFunctionException] should be thrownBy catalog.loadFunction(Identifier.of(Array("ns"), bucketFunctionName))
    catalog.listTables(Array.empty) shouldBe empty
    a[NoSuchTableException] should be thrownBy catalog.loadTable(Identifier.of(Array("ns"), "//tmp/table"))
  }

  it should "not support table mutations" in {
    val ident = Identifier.of(Array.empty, "//tmp/left")
    val newIdent = Identifier.of(Array.empty, "//tmp/right")
    val noProperties = Collections.emptyMap[String, String]()
    Seq[(String, () => Any)](
      s"CREATE TABLE is not supported for $ident" ->
        (() => catalog.createTable(ident, StructType(Nil), Array.empty[Transform], noProperties)),
      s"ALTER TABLE is not supported for $ident" -> (() => catalog.alterTable(ident)),
      s"DROP TABLE is not supported for $ident" -> (() => catalog.dropTable(ident)),
      s"RENAME TABLE is not supported for $ident to $newIdent" -> (() => catalog.renameTable(ident, newIdent))
    ).foreach { case (operation, call) =>
      val error = the[UnsupportedOperationException] thrownBy call()
      error.getMessage shouldBe s"$operation by the 'ytsaurus' catalog"
    }
  }
}
