package tech.ytsaurus.spyt.format.bucketing

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Optional

/*
 * Golden vectors produced by a YTsaurus cluster itself, never by the code under test: the argument rows were
 * inserted into a dynamic table (i, i2, i3: int64, u: uint64, b: boolean, s, s2: string) as raw text YSON, so the
 * string cases carry exactly the listed bytes (UTF-8 for the Unicode ones, 0x00 and invalid UTF-8 for the binary
 * ones), and `select_rows("..., farm_hash(<columns>) as h from [<table>]")` returned the uint64 values pinned here.
 * Results are compared as unsigned strings. The encoding contract (bit-cast int64, uint64/boolean/null/string,
 * seed 0xdeadc0de fold, ^ argument count) is documented on HashFunction; ("ab", "c"), ("a", "bc") and "abc"
 * hashing differently shows it is not concatenation.
 */
class HashFunctionTest extends AnyFlatSpec with Matchers {

  private def unsigned(value: Long): String = java.lang.Long.toUnsignedString(value)

  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def bytes(values: Int*): Array[Byte] = values.map(_.toByte).toArray

  private def pattern(length: Int): Array[Byte] = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    utf8((alphabet * (length / alphabet.length + 1)).take(length))
  }

  private def hash(argument: Any): Long = HashFunction.FARM_HASH.hash(argument.asInstanceOf[AnyRef])

  private def hashArguments(arguments: Any*): Long = {
    HashFunction.FARM_HASH.hashArguments(arguments.map(_.asInstanceOf[AnyRef]): _*)
  }

  it should "resolve the YTsaurus function name regardless of case" in {
    Seq("farm_hash", "FARM_HASH", "Farm_Hash").foreach { name =>
      withClue(s"[$name]: ") {
        HashFunction.byYtName(name) shouldEqual Optional.of(HashFunction.FARM_HASH)
      }
    }
  }

  it should "not resolve an unsupported or missing function name" in {
    Seq("hash", "farm_hash64", "farmhash", "", "farm_hash ", null).foreach { name =>
      withClue(s"[$name]: ") {
        HashFunction.byYtName(name) shouldEqual Optional.empty[HashFunction]()
      }
    }
  }

  it should "expose the canonical YTsaurus function name" in {
    HashFunction.FARM_HASH.ytName shouldEqual "farm_hash"
  }

  it should "hash an int64 argument exactly as YTsaurus farm_hash(i) does" in {
    val vectors = Seq(
      0L -> "3315701238936582721",
      1L -> "933529381690826619",
      2L -> "3427386618069609762",
      4L -> "15016036017421091022",
      42L -> "8164349598829955404",
      -1L -> "7129902556246829656",
      -42L -> "16397089221717914258",
      1000000007L -> "10215119506177658240",
      Long.MinValue -> "17576000849569414181",
      Long.MaxValue -> "17586188531206532588"
    )
    vectors.foreach { case (argument, expected) =>
      withClue(s"farm_hash($argument): ") {
        unsigned(hash(argument)) shouldEqual expected
      }
    }
  }

  it should "hash uint64, boolean and null arguments as YTsaurus does" in {
    // uint64 2^63 and 2^64-1 share the bit patterns of Long.MinValue and -1L; boolean is 0/1; null is 0
    unsigned(hash(Long.MinValue)) shouldEqual "17576000849569414181"
    unsigned(hash(-1L)) shouldEqual "7129902556246829656"
    unsigned(hash(true)) shouldEqual "933529381690826619"
    unsigned(hash(false)) shouldEqual "3315701238936582721"
    unsigned(hash(null)) shouldEqual "3315701238936582721"
  }

  it should "hash narrower integers as the int64 they are stored as" in {
    unsigned(hash(42: Int)) shouldEqual "8164349598829955404"
    unsigned(hash(42: Short)) shouldEqual "8164349598829955404"
    unsigned(hash(-1: Byte)) shouldEqual "7129902556246829656"
  }

  it should "hash a string argument exactly as YTsaurus farm_hash(s) does" in {
    val vectors = Seq[(String, Array[Byte], String)](
      ("empty", Array.empty[Byte], "18213160808259878197"),
      ("ascii abc", utf8("abc"), "8869018260447729478"),
      ("ascii hello_world", utf8("hello_world"), "2025645983239056574"),
      ("ascii MDwhat?", utf8("MDwhat?"), "7161823713280713685"),
      ("utf-8 cyrillic", utf8("Привет, мир"), "8312061054975202923"),
      ("utf-8 cjk", utf8("日本語"), "8550878968931154573"),
      ("embedded 0x00", bytes('a', 0x00, 'b'), "506347438330535169"),
      ("single 0x00", bytes(0x00), "8916873149217111397"),
      ("four 0x00", bytes(0x00, 0x00, 0x00, 0x00), "9682821703575156992"),
      ("invalid utf-8 with 0x00", bytes(0xff, 0xfe, 0x00, 0x01), "13608768642813757477"),
      ("16 bytes", utf8("x" * 16), "1018863032141701326"),
      ("17 bytes", utf8("y" * 17), "10873447293606436884"),
      ("32 bytes", pattern(32), "15718724012517393646"),
      ("33 bytes", pattern(33), "8396789985032708545"),
      ("64 bytes", pattern(64), "18377402886266701550"),
      ("65 bytes", pattern(65), "12792382431158111648"),
      ("200 bytes", pattern(200), "16806011184363067985"),
      ("1024 bytes", pattern(1024), "18015370968023235982"),
      ("4096 bytes", pattern(4096), "5786369983920396358")
    )
    vectors.foreach { case (clue, argument, expected) =>
      withClue(s"farm_hash($clue): ") {
        unsigned(hash(argument)) shouldEqual expected
        unsigned(hash(UTF8String.fromBytes(argument))) shouldEqual expected
      }
    }
    unsigned(hash("Привет, мир")) shouldEqual "8312061054975202923"
  }

  it should "hash a Catalyst UTF8String from its raw bytes" in {
    unsigned(hash(UTF8String.fromString("abc"))) shouldEqual "8869018260447729478"
    unsigned(hash(UTF8String.fromString("Привет, мир"))) shouldEqual "8312061054975202923"
    unsigned(hash(UTF8String.fromString("日本語"))) shouldEqual "8550878968931154573"
    unsigned(hash(UTF8String.fromBytes(bytes('a', 0x00, 'b')))) shouldEqual "506347438330535169"
    unsigned(hash(UTF8String.fromBytes(bytes(0xff, 0xfe, 0x00, 0x01)))) shouldEqual "13608768642813757477"
    // a slice of a larger buffer hashes the same as a standalone copy of its bytes
    val sliced = UTF8String.fromString("xxabcxx").substring(2, 5)
    hash(sliced) shouldEqual hash("abc")
    hash(sliced) shouldEqual hash(utf8("abc"))
    unsigned(hashArguments(1L, UTF8String.fromString("abc"))) shouldEqual "12535759425065966635"
  }

  it should "hash several arguments exactly as YTsaurus farm_hash(a, b, ...) does" in {
    val vectors = Seq[(String, Seq[Any], String)](
      ("farm_hash(1, 'abc')", Seq(1L, "abc"), "12535759425065966635"),
      ("farm_hash(-42, 'hello_world')", Seq(-42L, "hello_world"), "534301023022786"),
      ("farm_hash(0, '')", Seq(0L, ""), "10248854568006048452"),
      ("farm_hash(1, '')", Seq(1L, ""), "1965066217078014026"),
      ("farm_hash(42, 'a\\0b')", Seq(42L, bytes('a', 0x00, 'b')), "13636760347702089683"),
      ("farm_hash('abc', 1)", Seq("abc", 1L), "8492257055184476474"),
      ("farm_hash('hello_world', -42)", Seq("hello_world", -42L), "11728979308152564427"),
      ("farm_hash('', 0)", Seq("", 0L), "6870509494920195751"),
      ("farm_hash('a\\0b', 42)", Seq(bytes('a', 0x00, 'b'), 42L), "4715967167405623758"),
      ("farm_hash('', 1)", Seq("", 1L), "1689693841827987259"),
      ("farm_hash(1, 2)", Seq(1L, 2L), "3849326666456637875"),
      ("farm_hash(2, 1)", Seq(2L, 1L), "16318345755257926203"),
      ("farm_hash(42, -1)", Seq(42L, -1L), "15606905225410890809"),
      ("farm_hash(0, 0)", Seq(0L, 0L), "10793565175019682757"),
      ("farm_hash(1, 2, 3)", Seq(1L, 2L, 3L), "9356478348297141860"),
      ("farm_hash(3, 2, 1)", Seq(3L, 2L, 1L), "14156295371990732579"),
      ("farm_hash(0, 0, 0)", Seq(0L, 0L, 0L), "11786296186984197942"),
      ("farm_hash(null, 'abc')", Seq(null, "abc"), "5695727612511534157")
    )
    vectors.foreach { case (clue, arguments, expected) =>
      withClue(s"$clue: ") {
        unsigned(hashArguments(arguments: _*)) shouldEqual expected
      }
    }
  }

  it should "combine string arguments structurally rather than by concatenation" in {
    val vectors = Seq[(String, Seq[Any], String)](
      ("farm_hash('ab', 'c')", Seq("ab", "c"), "1854630625886482275"),
      ("farm_hash('a', 'bc')", Seq("a", "bc"), "2731265574099516655"),
      ("farm_hash('abc', '')", Seq("abc", ""), "6524561468729588652"),
      ("farm_hash('', 'abc')", Seq("", "abc"), "13401539836751996243"),
      ("farm_hash('', '')", Seq("", ""), "13904242047640012305"),
      ("farm_hash('abc', 'hello_world')", Seq("abc", "hello_world"), "14072603354312202161")
    )
    vectors.foreach { case (clue, arguments, expected) =>
      withClue(s"$clue: ") {
        unsigned(hashArguments(arguments: _*)) shouldEqual expected
      }
    }
    val concatenation = unsigned(hash("abc"))
    concatenation shouldEqual "8869018260447729478"
    vectors.map(_._3) should not contain concatenation
  }

  it should "hash an empty argument list as YTsaurus farm_hash() does" in {
    unsigned(hashArguments()) shouldEqual "3735929054"
  }

  it should "hash a single argument the same way through both entry points" in {
    Seq[Any](42L, "abc", bytes('a', 0x00, 'b'), true, null).foreach { argument =>
      withClue(s"[$argument]: ") {
        hash(argument) shouldEqual hashArguments(argument)
      }
    }
  }

  it should "reject argument types that YTsaurus farm_hash does not accept" in {
    Seq[Any](1.5d, 1.5f, BigInt(1), Seq(1L), 'c', Array[AnyRef]("a"), Array(1L)).foreach { argument =>
      withClue(s"[${argument.getClass.getSimpleName}]: ") {
        an[IllegalArgumentException] should be thrownBy hash(argument)
        an[IllegalArgumentException] should be thrownBy hashArguments(1L, argument)
      }
    }
  }

  it should "resolve the Java overloads to the documented contract" in {
    unsigned(HashFunctionJavaCalls.nullLiteral()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.nullObject()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.singleNullArgument()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.typedNullBytes()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.typedNullBytesAsObject()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.typedNullBytesAsArgument()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.typedNullString()) shouldEqual "3315701238936582721"
    unsigned(HashFunctionJavaCalls.bytes(utf8("abc"))) shouldEqual "8869018260447729478"
    unsigned(HashFunctionJavaCalls.primitiveLong(42L)) shouldEqual "8164349598829955404"
    unsigned(HashFunctionJavaCalls.spread(Array[AnyRef](Long.box(1L), "abc"))) shouldEqual "12535759425065966635"
    unsigned(HashFunctionJavaCalls.two(1L, "abc")) shouldEqual "12535759425065966635"
    unsigned(HashFunctionJavaCalls.noArguments()) shouldEqual "3735929054"
    an[IllegalArgumentException] should be thrownBy HashFunctionJavaCalls.objectArrayAsSingleArgument(Array[AnyRef]("a"))
    val missingArray = the[NullPointerException] thrownBy HashFunctionJavaCalls.nullArray()
    missingArray.getMessage should include("null")
  }
}
