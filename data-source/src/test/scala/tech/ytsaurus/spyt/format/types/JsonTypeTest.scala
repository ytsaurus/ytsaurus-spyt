package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.TiType

class JsonTypeTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  private val ids = Seq(1, 2)
  private val names = Seq("Ann", "Bob")

  private val jsonsUsers = ids.zip(names).map(x => s"""{"id": ${x._1}, "name": "${x._2}"}""")

  private val jsonComplex =
    """{
      |"id": 3,
      |"values": [
      |{"a": 1, "b": 4},
      |{"a": 1, "b": 9}
      |]
      |}""".stripMargin

  private val jsonsAll = jsonsUsers ++ Seq(jsonComplex)

  private def generateTestDataframe(data: Seq[String]) = {
    val tableSchema: TableSchema = TableSchema.builder()
      .addValue("json_field", TiType.json())
      .build()

    writeTableFromURow(data.map(packToRow), tmpPath, tableSchema)
    spark.read.yt(tmpPath)
  }

  private def packToRow(value: String): UnversionedRow = {
    new UnversionedRow(java.util.List.of[UnversionedValue](
      new UnversionedValue(0, ColumnValueType.STRING, false, value.getBytes())
    ))
  }

  import spark.implicits._

  it should "read yt Json type to spark StringType schema" in {
    val df = generateTestDataframe(jsonsAll)
    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs Seq(
      StructField("json_field", StringType)
    )
  }

  it should "read yt Json data to spark dataframe" in {
    val df = generateTestDataframe(jsonsAll)
    val rows_all = jsonsAll.map(Row(_))
    df.collect() should contain theSameElementsAs rows_all
  }

  it should "manipulate loaded from yt json data in spark dataframe" in {
    val df = generateTestDataframe(jsonsUsers)
    val userSchema = StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType)
    ))

    val parsedDF = df.select(from_json($"json_field", userSchema).alias("parsed_json"))

    val parsedIds = parsedDF.select("parsed_json.id").collect().map(_.getLong(0))
    parsedIds should contain theSameElementsAs ids

    val parsedNames = parsedDF.select("parsed_json.name").collect().map(_.getString(0))
    parsedNames should contain theSameElementsAs names
  }

  it should "write Json data to a table with json column using schema hint" in {
    val df = jsonsAll.toDF("json_field")
    df.write
      .schemaHint(Map("json_field" -> YtLogicalType.Json))
      .option("write_type_v3", "True")
      .yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
    schema.getColumnNames should contain theSameElementsAs Seq("json_field")
    schema.getColumnNames should have size 1
    schema.getColumnType(0) shouldEqual ColumnValueType.STRING
    typeV3(tmpPath, "json_field") shouldEqual "json"

    val data = readTableAsYson(tmpPath).map(_.asMap().get("json_field").stringValue())

    data should contain theSameElementsAs jsonsAll
  }
}
