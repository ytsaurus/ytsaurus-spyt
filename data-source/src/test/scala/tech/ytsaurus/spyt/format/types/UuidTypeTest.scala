package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.types.{Metadata, StringType, StructField}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.common.utils.UuidUtils
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.{YtReader, YtWriter}
import tech.ytsaurus.typeinfo.TiType

class UuidTypeTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  private val uuids = Seq(
    "4b336d46-6576-4c7a-5436-4f6847644642",
    "4b336d01-6576-4c7a-5436-4f6801644642"
  )

  private val ytUuids = uuids.map(UuidUtils.uuidToBytes)

  import spark.implicits._

  it should "read a table with uuid column to Uuid" in {
    val tableSchema = TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("uuid_field", TiType.uuid())
      .build()

    writeTableFromURow(ytUuids.map(x => packToRow(x)), tmpPath, tableSchema)

    val df = spark.read.yt(tmpPath)

    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs
      Seq(StructField("uuid_field", StringType))

    df.select("uuid_field").collect().map(_.getString(0)) should
      contain theSameElementsAs uuids
  }


  it should "write Uuid data to a table with uuid column using schema hint" in {

    val df = uuids.toDF("uuid_field")
    df.write
      .schemaHint(Map("uuid_field" -> YtLogicalType.Uuid))
      .option("write_type_v3","True")
      .yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
    schema.getColumnNames should contain theSameElementsAs Seq("uuid_field")
    schema.getColumnNames should have size 1
    schema.getColumnType(0) shouldEqual ColumnValueType.STRING
    typeV3(tmpPath, "uuid_field") shouldEqual "uuid"

    val data = readTableAsYson(tmpPath).map(_.asMap().get("uuid_field").stringValue())

    data.map(s => UuidUtils.bytesToUuid(s.getBytes)) should contain theSameElementsAs uuids
  }

  private def packToRow(value: Array[Byte]): UnversionedRow = {
    new UnversionedRow(java.util.List.of[UnversionedValue](
      new UnversionedValue(0, ColumnValueType.STRING, false, value)
    ))
  }
}
