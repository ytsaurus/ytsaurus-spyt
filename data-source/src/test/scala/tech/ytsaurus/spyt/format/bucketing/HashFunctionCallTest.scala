package tech.ytsaurus.spyt.format.bucketing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{Arrays, Collections, Optional}

class HashFunctionCallTest extends AnyFlatSpec with Matchers {

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

  it should "bucket a hash by its unsigned remainder, as YTsaurus does for uint64, for any positive bucket count" in {
    val hash = HashFunction.FARM_HASH.hash(Long.box(4L))
    hash should be < 0L
    HashFunctionCall.bucketOf(hash, 100) shouldEqual 22L
    // farm_hash(4) % 2147483647, the uint64 hash 15016036017421091022 taken unsigned
    HashFunctionCall.bucketOf(hash, Int.MaxValue) shouldEqual 832723767L
    Seq(0, -1, Int.MinValue).foreach { buckets =>
      withClue(s"[$buckets]: ") {
        val error = the[IllegalArgumentException] thrownBy HashFunctionCall.bucketOf(1L, buckets)
        error.getMessage should include(buckets.toString)
      }
    }
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
