package tech.ytsaurus.spyt.serializers

import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}

class ExtendedDataFrameSerializerTest  extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {

  private val schema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("id", ColumnValueType.INT64)
    .addValue("description", ColumnValueType.STRING)
    .addValue("value", ColumnValueType.UINT64)
    .build()

  it should "serialize uint64 types using extended YTsaurus types" in {
    writeTableFromYson(Seq(
      """{id = 1; description = "a"; value = 1u; }""",
      """{id = 2; description = "b"; value = 9223372036854775816u; }"""
    ), tmpPath, schema)

    val result = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(result)
    val answer = Array(
      "rAAAAAAAAAAKMgoCaWQQA0gAWih7InR5cGVfbmFtZSI9Im9wdGlvbmFsIjsiaXRlbSI9ImludDY0Ijt9CjwKC2Rlc2NyaXB0aW9uEBBIAF" +
        "opeyJ0eXBlX25hbWUiPSJvcHRpb25hbCI7Iml0ZW0iPSJzdHJpbmciO30KNgoFdmFsdWUQBEgAWil7InR5cGVfbmFtZSI9Im9wdGlvbm" +
        "FsIjsiaXRlbSI9InVpbnQ2NCI7fRgAAAAAAAIAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAABhAAAAAAAAAAEAAA" +
        "AAAAAAAwAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAQAAAAAAAABiAAAAAAAAAAgAAAAAAACA"
    )
    tableBytes should contain theSameElementsAs answer
  }
}
