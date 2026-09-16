package tech.ytsaurus.spyt.format.bucketing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{Arrays, Collections, Optional}

class HashFunctionCallTest extends AnyFlatSpec with Matchers {

  private def int64(value: Long): java.lang.Long = Long.box(value)

  private def columns(names: String*): java.util.List[String] = Arrays.asList(names: _*)

  it should "expose the function, the columns and the optional bucket count" in {
    val plain = new HashFunctionCall(HashFunction.FARM_HASH, columns("region", "user_id"))
    plain.function shouldEqual HashFunction.FARM_HASH
    plain.arguments shouldEqual columns("region", "user_id")
    plain.buckets shouldEqual Optional.empty[Integer]()
    new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 8).buckets shouldEqual Optional.of(8)
  }

  it should "refuse a missing function or column list" in {
    a[NullPointerException] should be thrownBy new HashFunctionCall(null, columns("k"))
    a[NullPointerException] should be thrownBy new HashFunctionCall(null, columns("k"), 8)
    a[NullPointerException] should be thrownBy new HashFunctionCall(HashFunction.FARM_HASH, null)
    a[NullPointerException] should be thrownBy new HashFunctionCall(HashFunction.FARM_HASH, null, 8)
  }

  it should "refuse a null column name" in {
    an[IllegalArgumentException] should be thrownBy
      new HashFunctionCall(HashFunction.FARM_HASH, Arrays.asList("k", null))
    an[IllegalArgumentException] should be thrownBy
      new HashFunctionCall(HashFunction.FARM_HASH, Collections.singletonList[String](null), 8)
  }

  it should "refuse a bucket count that is not positive" in {
    Seq(0, -1, -8, Int.MinValue).foreach { buckets =>
      withClue(s"[$buckets]: ") {
        val error = the[IllegalArgumentException] thrownBy
          new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), buckets)
        error.getMessage should include(buckets.toString)
      }
    }
    new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 1).buckets shouldEqual Optional.of(1)
    new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), Int.MaxValue).buckets shouldEqual Optional.of(Int.MaxValue)
  }

  it should "hash and bucket values in column order the way YTsaurus does" in {
    val call = new HashFunctionCall(HashFunction.FARM_HASH, columns("user_id", "region"), 10)
    java.lang.Long.toUnsignedString(call.hash(int64(1L), "abc")) shouldEqual "12535759425065966635"
    call.bucket(int64(1L), "abc") shouldEqual 5L
    new HashFunctionCall(HashFunction.FARM_HASH, columns("region", "user_id"), 10).bucket("abc", int64(1L)) shouldEqual 4L
  }

  it should "take the bucket as the unsigned remainder, as YTsaurus does for uint64" in {
    val call = new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 100)
    val hash = call.hash(int64(4L))
    hash should be < 0L
    call.bucket(int64(4L)) shouldEqual 22L
    hash % 100L shouldEqual -94L
  }

  it should "refuse to evaluate the wrong number of values or a missing value array" in {
    val call = new HashFunctionCall(HashFunction.FARM_HASH, columns("user_id", "region"), 10)
    an[IllegalArgumentException] should be thrownBy call.hash(int64(1L))
    an[IllegalArgumentException] should be thrownBy call.hash(int64(1L), "abc", "extra")
    an[IllegalArgumentException] should be thrownBy call.bucket()
    a[NullPointerException] should be thrownBy HashFunctionJavaCalls.callWithNullValues(call)
  }

  it should "hash a null value as YTsaurus null" in {
    val call = new HashFunctionCall(HashFunction.FARM_HASH, columns("k"))
    java.lang.Long.toUnsignedString(call.hash(null.asInstanceOf[AnyRef])) shouldEqual "3315701238936582721"
  }

  it should "refuse to bucket a call without a bucket count" in {
    an[IllegalStateException] should be thrownBy
      new HashFunctionCall(HashFunction.FARM_HASH, columns("k")).bucket(int64(42L))
  }

  it should "compare by function, columns and bucket count" in {
    val call = new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 8)
    call shouldEqual new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 8)
    call.hashCode shouldEqual new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 8).hashCode
    call should not equal new HashFunctionCall(HashFunction.FARM_HASH, columns("k"), 16)
    call should not equal new HashFunctionCall(HashFunction.FARM_HASH, columns("k"))
    call should not equal new HashFunctionCall(HashFunction.FARM_HASH, columns("K"), 8)
    call.toString shouldEqual "HashFunctionCall(FARM_HASH, [k], Optional[8])"
    val plain = new HashFunctionCall(HashFunction.FARM_HASH, columns("k"))
    plain shouldEqual new HashFunctionCall(HashFunction.FARM_HASH, columns("k"))
    plain.hashCode shouldEqual new HashFunctionCall(HashFunction.FARM_HASH, columns("k")).hashCode
    plain.toString shouldEqual "HashFunctionCall(FARM_HASH, [k], Optional.empty)"
  }
}
