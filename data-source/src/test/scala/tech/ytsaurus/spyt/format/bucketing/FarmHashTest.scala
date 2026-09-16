package tech.ytsaurus.spyt.format.bucketing

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

// FarmHash building blocks. The raw fingerprint and pair-combiner values are the ones YTsaurus pins in its own
// FarmFingerprint stability tests; the folded values were produced by YTsaurus select_rows the way
// HashFunctionTest describes.
class FarmHashTest extends AnyFlatSpec with Matchers {

  private def unsigned(value: Long): String = java.lang.Long.toUnsignedString(value)

  private def fp(value: Long): Long = FarmHash.fingerprint(value)

  private def fp(value: String): Long = FarmHash.fingerprintBytes(value.getBytes(StandardCharsets.UTF_8))

  it should "reproduce the YTsaurus 64 bit fingerprint of an integer value" in {
    unsigned(FarmHash.fingerprint(42L)) shouldEqual "17355217915646310598"
  }

  it should "reproduce the YTsaurus 64 bit fingerprint of a byte string" in {
    unsigned(FarmHash.fingerprintBytes("MDwhat?".getBytes(StandardCharsets.UTF_8))) shouldEqual "10997514911242581312"
  }

  it should "reproduce the YTsaurus fingerprint pair combiner" in {
    unsigned(FarmHash.combine(1234L, 5678L)) shouldEqual "16769064555670434975"
  }

  it should "fold fingerprints over an argument list the way farm_hash does" in {
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

  it should "fold an empty argument list into the bare seed" in {
    unsigned(FarmHash.fold()) shouldEqual "3735929054"
  }

  it should "be the building block HashFunction folds a single argument with" in {
    FarmHash.fold(fp(42L)) shouldEqual HashFunction.FARM_HASH.hash(Long.box(42L))
    FarmHash.fold(fp("abc")) shouldEqual HashFunction.FARM_HASH.hash("abc")
  }
}
