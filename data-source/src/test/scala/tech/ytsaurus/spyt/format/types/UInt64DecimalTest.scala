package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.types.{Decimal, DecimalType, Metadata, StringType, StructField}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.spyt.types.YTsaurusTypes

class UInt64DecimalTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  import spark.implicits._

  it should "read a table with uint64 column to Decimal" in {
    val tableSchema = TableSchema.builder()
      .addValue("id", ColumnValueType.UINT64)
      .addValue("value", ColumnValueType.STRING)
      .build()

    writeTableFromYson(Seq(
      """{id = 1u; value = "value 1"}""",
      """{id = 2u; value = "value 2"}""",
      """{id = 3u; value = "value 3"}""",
      """{id = 9223372036854775816u; value = "value 4"}""",
      """{id = 9223372036854775813u; value = "value 5"}""",
      """{id = 18446744073709551615u; value = "value 6"}""" // 2^64 - 1
    ), tmpPath, tableSchema)

    val df = spark.read.yt(tmpPath)
    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs Seq(
      StructField("id", YTsaurusTypes.UINT64_DEC_TYPE),
      StructField("value", StringType)
    )
    df.select("id").as[BigDecimal].collect() should contain theSameElementsAs Seq(
      BigDecimal(1L), BigDecimal(2L), BigDecimal(3L), BigDecimal("9223372036854775813"),
      BigDecimal("9223372036854775816"), BigDecimal("18446744073709551615")
    )
  }

  it should "write Decimal to a table with uint64 column using schema hint" in {
    val df = Seq(
      BigDecimal(1L), BigDecimal(2L), BigDecimal(3L), BigDecimal("9223372036854775813"),
      BigDecimal("9223372036854775816"), BigDecimal("18446744073709551615")
    ).toDF("a")
    df.write.schemaHint(Map("a" -> YtLogicalType.Uint64)).yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
    schema.getColumnNames should contain theSameElementsAs Seq("a")
    schema.getColumnNames should have size 1
    schema.getColumnType(0) shouldEqual ColumnValueType.UINT64

    val data = readTableAsYson(tmpPath).map(_.asMap().get("a").longValue())
    val expected = Seq(1L, 2L, 3L, java.lang.Long.parseUnsignedLong("9223372036854775813"),
      java.lang.Long.parseUnsignedLong("9223372036854775816"), java.lang.Long.parseUnsignedLong("18446744073709551615"))

    data should contain theSameElementsAs expected
  }
}
