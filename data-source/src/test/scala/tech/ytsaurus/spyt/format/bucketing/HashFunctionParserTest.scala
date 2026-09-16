package tech.ytsaurus.spyt.format.bucketing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.{Arrays, Optional}

class HashFunctionParserTest extends AnyFlatSpec with Matchers {

  private def int64(value: Long): java.lang.Long = Long.box(value)

  private def plainCall(columns: String*): Optional[HashFunctionCall] = {
    Optional.of(new HashFunctionCall(HashFunction.FARM_HASH, Arrays.asList(columns: _*)))
  }

  private def bucketCall(buckets: Int, columns: String*): Optional[HashFunctionCall] = {
    Optional.of(new HashFunctionCall(HashFunction.FARM_HASH, Arrays.asList(columns: _*), buckets))
  }

  private def rejected(expressions: String*): Unit = {
    expressions.foreach { expression =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual Optional.empty[HashFunctionCall]()
      }
    }
  }

  it should "resolve the parsed call to the hash function itself" in {
    HashFunctionParser.parse("farm_hash(k) % 8").get.function shouldEqual HashFunction.FARM_HASH
  }

  it should "parse the function name regardless of case, as YTsaurus does" in {
    val expressions = Seq(
      "FARM_HASH(k)" -> plainCall("k"),
      "Farm_Hash(k)" -> plainCall("k"),
      "FARM_HASH(k) % 8" -> bucketCall(8, "k"),
      "Farm_hash(region, user_id) % 4" -> bucketCall(4, "region", "user_id")
    )
    expressions.foreach { case (expression, expected) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual expected
      }
    }
  }

  it should "keep column identifiers case-sensitive" in {
    HashFunctionParser.parse("farm_hash(K)") shouldEqual plainCall("K")
    HashFunctionParser.parse("farm_hash(User_Id, region) % 8") shouldEqual bucketCall(8, "User_Id", "region")
  }

  it should "parse a plain farm_hash call" in {
    val expressions = Seq(
      "farm_hash(k)" -> "k",
      "farm_hash( k )" -> "k",
      "  farm_hash(k)  " -> "k",
      "farm_hash(user_id)" -> "user_id"
    )
    expressions.foreach { case (expression, column) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual plainCall(column)
      }
    }
  }

  it should "parse a bucketized farm_hash call" in {
    val expressions = Seq(
      "farm_hash(k) % 8" -> 8,
      "farm_hash(k)%8" -> 8,
      "farm_hash( k )  %  16" -> 16,
      "  farm_hash(k) % 1  " -> 1,
      "farm_hash(k) % 2147483647" -> 2147483647
    )
    expressions.foreach { case (expression, buckets) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual bucketCall(buckets, "k")
      }
    }
  }

  it should "parse a uint64 bucket literal with the lower-case u suffix of the YTsaurus lexer" in {
    val expressions = Seq(
      "farm_hash(k) % 100u" -> 100,
      "farm_hash(k) % 1u" -> 1,
      "farm_hash(k)%8u" -> 8,
      "farm_hash(k) % 2147483647u" -> 2147483647,
      "farm_hash(a, b) % 16u" -> 16
    )
    expressions.foreach { case (expression, buckets) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression).get.buckets shouldEqual Optional.of(buckets)
      }
    }
  }

  it should "not recognize a bucket literal that is zero, overflows or carries a wrong suffix" in {
    rejected(
      "farm_hash(k) % 0u",
      "farm_hash(k) % 00u",
      "farm_hash(k) % 2147483648u",
      "farm_hash(k) % 18446744073709551615u",
      "farm_hash(k) % 100U",
      "farm_hash(k) % 10uu",
      "farm_hash(k) % 10 u",
      "farm_hash(k) % u8",
      "farm_hash(k) % 8l",
      "farm_hash(k) % 0x10",
      "farm_hash(k) % +8",
      "farm_hash(k) % 8.0"
    )
  }

  it should "parse a plain farm_hash call over several columns" in {
    val expressions = Seq(
      "farm_hash(region, user_id)" -> Seq("region", "user_id"),
      "farm_hash(region,user_id)" -> Seq("region", "user_id"),
      "farm_hash( region , user_id , day )" -> Seq("region", "user_id", "day")
    )
    expressions.foreach { case (expression, columns) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual plainCall(columns: _*)
      }
    }
  }

  it should "parse a bucketized farm_hash call over several columns" in {
    val expressions = Seq(
      "farm_hash(region, user_id) % 10" -> (Seq("region", "user_id"), 10),
      "farm_hash(a,b,c)%4" -> (Seq("a", "b", "c"), 4),
      "farm_hash(region,\n\tuser_id)\n%\t10" -> (Seq("region", "user_id"), 10),
      "farm_hash(info, true_flag) % 8" -> (Seq("info", "true_flag"), 8)
    )
    expressions.foreach { case (expression, (columns, buckets)) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual bucketCall(buckets, columns: _*)
      }
    }
  }

  it should "parse quoted column identifiers into their bare names" in {
    val expressions = Seq(
      "farm_hash([user-id]) % 8" -> bucketCall(8, "user-id"),
      "farm_hash(`user-id`) % 8" -> bucketCall(8, "user-id"),
      "farm_hash([k])" -> plainCall("k"),
      "farm_hash(`k`)" -> plainCall("k"),
      "farm_hash([a b], `c d`)" -> plainCall("a b", "c d"),
      "farm_hash([a,b], c) % 4" -> bucketCall(4, "a,b", "c"),
      "farm_hash(`a,b`)" -> plainCall("a,b"),
      "farm_hash(`a]b`)" -> plainCall("a]b"),
      "farm_hash([a`b])" -> plainCall("a`b"),
      "farm_hash([a\\b])" -> plainCall("a\\b"),
      "farm_hash(`a\\`b`)" -> plainCall("a`b"),
      "farm_hash(`a\\\\b`)" -> plainCall("a\\b"),
      "farm_hash(`a\\\"b\\'c`)" -> plainCall("a\"b'c"),
      "farm_hash(`true`, [null]) % 2" -> bucketCall(2, "true", "null"),
      "farm_hash(`select`)" -> plainCall("select"),
      "farm_hash( [k] ,`v` )%3u" -> bucketCall(3, "k", "v")
    )
    expressions.foreach { case (expression, expected) =>
      withClue(s"[$expression]: ") {
        HashFunctionParser.parse(expression) shouldEqual expected
      }
    }
  }

  it should "not recognize quoted identifiers outside the supported subset of the YTsaurus grammar" in {
    rejected(
      "farm_hash(\"k\") % 8",
      "farm_hash('k') % 8",
      "farm_hash([a[b]c]) % 8",
      "farm_hash([]) % 8",
      "farm_hash(``) % 8",
      "farm_hash([k) % 8",
      "farm_hash(`k) % 8",
      "farm_hash(k]) % 8",
      "farm_hash(`a\\x41b`) % 8",
      "farm_hash(`a\\nb`) % 8",
      "farm_hash(`a\\`) % 8",
      "farm_hash([k]x) % 8",
      "farm_hash(x[k]) % 8",
      "farm_hash(`k`[v]) % 8"
    )
  }

  it should "not recognize a missing expression" in {
    HashFunctionParser.parse(null) shouldEqual Optional.empty[HashFunctionCall]()
  }

  it should "not recognize an expression that is not a supported hash call" in {
    rejected(
      "",
      "   ",
      "farm_hash(k) % 0",
      "farm_hash(k) % -8",
      "farm_hash(k) % 8 + 1",
      "farm_hash(k) % 8.5",
      "hash(k) % 8",
      "int64(farm_hash(k) % 8)",
      "farm_hash(k) % 2147483648",
      "farm_hash(k) / 8",
      "farm_hash(k) %",
      "8 % farm_hash(k)",
      "farm_hash(k * 2) % 8",
      "farm_hash()",
      "farm_hash(a,)",
      "farm_hash(,a)",
      "farm_hash(a,, b)",
      "farm_hash(a b)",
      "farm_hash(a, 1) % 8",
      "farm_hash(a, b * 2) % 8",
      "farm_hash(TRUE)",
      "farm_hash(a, true) % 8",
      "farm_hash(a, False) % 8",
      "farm_hash(null) % 8",
      "farm_hash(a, NULL)",
      "farm_hash(k, inf) % 8",
      "farm_hash(k, %true) % 8",
      "farm_hash(k)) % 8",
      "(farm_hash(k)) % 8"
    )
  }

  it should "evaluate a parsed call end to end" in {
    // smoke vectors only; the full golden set lives in HashFunctionTest
    HashFunctionParser.parse("farm_hash(k) % 8").get.bucket(int64(42L)) shouldEqual 4L
    HashFunctionParser.parse("farm_hash(user_id, region) % 10").get.bucket(int64(1L), "abc") shouldEqual 5L
    HashFunctionParser.parse("farm_hash(region, user_id) % 10").get.bucket("abc", int64(1L)) shouldEqual 4L
    HashFunctionParser.parse("farm_hash([user-id]) % 100u").get.bucket(int64(42L)) shouldEqual 4L
  }
}
