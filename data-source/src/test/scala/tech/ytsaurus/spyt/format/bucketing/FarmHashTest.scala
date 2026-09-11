package tech.ytsaurus.spyt.format.bucketing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class FarmHashTest extends AnyFlatSpec with Matchers {

  private val Alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

  private def unsigned(value: Long): String = java.lang.Long.toUnsignedString(value)

  private def pattern(length: Int): String = (Alphabet * (length / Alphabet.length + 1)).take(length)

  private def fp(value: Long): Long = FarmHash.fingerprint(value)

  private def fp(value: String): Long = FarmHash.fingerprintBytes(value.getBytes(StandardCharsets.UTF_8))

  it should "reproduce the YTsaurus 64 bit fingerprint of an integer value" in {
    unsigned(FarmHash.fingerprint(42L)) shouldEqual "17355217915646310598"
  }

  it should "reproduce the YTsaurus fingerprint pair combiner" in {
    unsigned(FarmHash.combine(1234L, 5678L)) shouldEqual "16769064555670434975"
  }

  it should "reproduce farm_hash of a single int64 argument" in {
    val vectors = Seq(
      0L -> "3315701238936582721",
      1L -> "933529381690826619",
      2L -> "3427386618069609762",
      4L -> "15016036017421091022",
      42L -> "8164349598829955404",
      1000000007L -> "10215119506177658240",
      -1L -> "7129902556246829656",
      -42L -> "16397089221717914258"
    )
    vectors.foreach { case (argument, expected) =>
      withClue(s"farm_hash($argument): ") {
        unsigned(FarmHash.hashLong(argument)) shouldEqual expected
      }
    }
  }

  it should "reproduce the YTsaurus bucket of a single int64 argument" in {
    val buckets = 64L
    val vectors = Seq(
      0L -> 1L,
      1L -> 59L,
      42L -> 12L,
      -1L -> 24L,
      -42L -> 18L,
      1000000007L -> 0L
    )
    vectors.foreach { case (argument, expected) =>
      withClue(s"farm_hash($argument) % $buckets: ") {
        java.lang.Long.remainderUnsigned(FarmHash.hashLong(argument), buckets) shouldEqual expected
      }
    }
  }

  it should "reproduce the YTsaurus bucket for a bucket count that is not a power of two" in {
    val buckets = 100L
    val vectors = Seq(
      0L -> 21L,
      42L -> 4L,
      4L -> 22L,
      -1L -> 56L,
      -42L -> 58L
    )
    vectors.foreach { case (argument, expected) =>
      withClue(s"farm_hash($argument) % $buckets: ") {
        java.lang.Long.remainderUnsigned(FarmHash.hashLong(argument), buckets) shouldEqual expected
      }
    }
  }

  it should "differ from signed and floor modulo when the hash has its high bit set" in {
    val hash = FarmHash.hashLong(4L)
    hash should be < 0L
    java.lang.Long.remainderUnsigned(hash, 100L) shouldEqual 22L
    hash % 100L shouldEqual -94L
    Math.floorMod(hash, 100L) shouldEqual 6L
  }

  it should "reproduce the YTsaurus 64 bit fingerprint of a byte string" in {
    unsigned(FarmHash.fingerprintBytes("MDwhat?".getBytes(StandardCharsets.UTF_8))) shouldEqual "10997514911242581312"
  }

  it should "reproduce farm_hash of a single string argument for every length branch" in {
    val vectors = Seq(
      "abc" -> "8869018260447729478",
      "x" * 16 -> "1018863032141701326",
      "y" * 17 -> "10873447293606436884",
      pattern(32) -> "15718724012517393646",
      pattern(33) -> "8396789985032708545",
      pattern(64) -> "18377402886266701550",
      pattern(65) -> "12792382431158111648",
      pattern(200) -> "16806011184363067985",
      pattern(1024) -> "18015370968023235982",
      pattern(4096) -> "5786369983920396358"
    )
    vectors.foreach { case (argument, expected) =>
      withClue(s"farm_hash(string of ${argument.length} bytes): ") {
        unsigned(FarmHash.hashString(argument)) shouldEqual expected
      }
    }
  }

  it should "reproduce farm_hash of a string argument that is not valid ascii" in {
    val bytes: Array[Byte] = Array(
      0xc3, 0x90, 0xc2, 0xbf, 0xc3, 0x91,
      0xc2, 0x80, 0xc3, 0x90, 0xc2, 0xb8,
      0xc3, 0x90, 0xc2, 0xb2, 0xc3, 0x90,
      0xc2, 0xb5, 0xc3, 0x91, 0xc2, 0x82,
      0x5f, 0xc3, 0x90, 0xc2, 0xbc, 0xc3,
      0x90, 0xc2, 0xb8, 0xc3, 0x91, 0xc2,
      0x80
    ).map(_.toByte)
    bytes.length shouldEqual 37
    unsigned(FarmHash.hashBytes(bytes)) shouldEqual "10566156234387381815"
  }

  it should "reproduce farm_hash folded over an argument list" in {
    val vectors = Seq(
      ("farm_hash(1, 2)", Seq(fp(1L), fp(2L)), "3849326666456637875"),
      ("farm_hash(42, -1)", Seq(fp(42L), fp(-1L)), "15606905225410890809"),
      ("farm_hash(0, 0)", Seq(fp(0L), fp(0L)), "10793565175019682757"),
      ("farm_hash(1, 'abc')", Seq(fp(1L), fp("abc")), "12535759425065966635"),
      ("farm_hash(-42, 'hello_world')", Seq(fp(-42L), fp("hello_world")), "534301023022786"),
      ("farm_hash(1, 2, 3)", Seq(fp(1L), fp(2L), fp(3L)), "9356478348297141860"),
      ("farm_hash(0, 0, 0)", Seq(fp(0L), fp(0L), fp(0L)), "11786296186984197942"),
      ("farm_hash('abc', 'hello_world')", Seq(fp("abc"), fp("hello_world")), "14072603354312202161")
    )
    vectors.foreach { case (clue, fingerprints, expected) =>
      withClue(s"$clue: ") {
        unsigned(FarmHash.fold(fingerprints: _*)) shouldEqual expected
      }
    }
  }

  it should "fold a single argument into the pinned single argument hashes" in {
    FarmHash.fold(fp(42L)) shouldEqual FarmHash.hashLong(42L)
    unsigned(FarmHash.fold(fp(42L))) shouldEqual "8164349598829955404"
    FarmHash.fold(fp("abc")) shouldEqual FarmHash.hashString("abc")
    unsigned(FarmHash.fold(fp("abc"))) shouldEqual "8869018260447729478"
  }

  it should "fold an empty argument list into the bare seed" in {
    unsigned(FarmHash.fold()) shouldEqual "3735929054"
  }
}
