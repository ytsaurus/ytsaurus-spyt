package tech.ytsaurus.spyt.serializers

import NYT.NTableClient.NProto.ChunkMeta.TTableSchemaExt
import org.apache.commons.codec.binary.Hex
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}

class DataFrameSerializerTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with TestUtils {
  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val anySchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("value", ColumnValueType.ANY)
    .build()

  private val nonLatinSchema = TableSchema.builder()
    .addValue("id", ColumnValueType.INT64)
    .addValue("value", ColumnValueType.STRING)
    .build()

  it should "serialize dataframe to byte array" in {
    writeTableFromYson(Seq(
      """{a = 0; b = #; c = 0.0}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormat(res)
    val answer = Array(Array(55, 0, 0, 0, 0, 0, 0, 0, 10, 15, 10, 1, 97, 16, 3, 64, 3, 72, 0, 82, 4, 18, 2, 8, 3, 10,
      15, 10, 1, 98, 16, 16, 64, 16, 72, 0, 82, 4, 18, 2, 8, 16, 10, 15, 10, 1, 99, 16, 5, 64, 5, 72, 0, 82, 4, 18,
      2, 8, 5, 16, 1, 24, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize dataframe to base64" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = Array(
      "NwAAAAAAAAAKDwoBYRADQANIAFIEEgIIAwoPCgFiEBBAEEgAUgQSAggQCg8KAWMQBUAFSABSBBICCAUQARgAAAIAAAAAAAAAAw" +
        "AAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAQAAAAAAAABhAAAAAAAAADMzMzMzM9M/AwAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAQ" +
        "AAAAAAAABiAAAAAAAAAAAAAAAAAOA/"
    )
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize lists" in {
    writeTableFromYson(Seq(
      "{value = [[1]; [2; 3]]}",
      "{value = [[4]; #]}"
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> ArrayType(ArrayType(LongType)))
      .yt(tmpPath)

    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = Array(
      "HwAAAAAAAAAKGQoFdmFsdWUQEUgAUgwSChoIEgYaBBICCAMQARgAAAIAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA4AAAAAAAAAW1" +
        "sCAl07WwIEOwIGXV0AAAEAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAFtbAghdOyNd"
    )
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize map" in {
    writeTableFromYson(Seq(
      """{value = [[1; "2"]; [3; "4"]]}""",
      """{value = [[5; "6"]; [7; "8"]]}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(LongType, StringType))
      .yt(tmpPath)

    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = Array(
      "IQAAAAAAAAAKGwoFdmFsdWUQEUgAUg4SDEIKCgIIAxIEEgIIEBABGAAAAAAAAAAAAgAAAAAAAAABAAAAAAAAAAAAAAAAAAAAEwAAAAAA" +
        "AABbWwIGOwECNF07WwICOwECMl1dAAAAAAABAAAAAAAAAAAAAAAAAAAAEwAAAAAAAABbWwIOOwECOF07WwIKOwECNl1dAAAAAAA="
    )
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize non-latin symbols in unicode" in {
    writeTableFromYson(Seq(
      """{id = 1; value = "Номер один"}""",
      """{id = 2; value = "Номер два"}"""
    ), tmpPath, nonLatinSchema)

    val res = spark.read.yt(tmpPath)
    val resultBase64 = GenericRowSerializer.dfToYTFormatWithBase64(res)

    resultBase64 should contain theSameElementsAs Seq(
      "KwAAAAAAAAAKEAoCaWQQA0ADSABSBBICCAMKEwoFdmFsdWUQEEAQSABSBBICCBAQARgAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAA" +
        "AAABAAAAAAAAABMAAAAAAAAA0J3QvtC80LXRgCDQvtC00LjQvQAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAEQAAAAAAAADQnd" +
        "C+0LzQtdGAINC00LLQsAAAAAAAAAA="
    )
  }

  it should "serialize decimal values" in {
    import spark.implicits._

    val df = spark.createDataset(Seq((1L, 10.0), (2L, 10.2), (3L, 10.003)))
      .toDF().select($"_1".as("id"), $"_2".cast(DecimalType(9, 4)).as("value"))

    val resultBase64 = GenericRowSerializer.dfToYTFormatWithBase64(df)
    resultBase64 should contain theSameElementsAs Seq(
      "KwAAAAAAAAAKDgoCaWQQA0ADSAFSAggDChUKBXZhbHVlEBFIAFIIEgZSBAgJEAQQARgAAAAAAAADAAAAAAAAAAIAAAAAAAAAAAAAAAAAA" +
        "AABAAAAAAAAAAQAAAAAAAAAgAGGoAAAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAEAAAAAAAAAIABjnAAAAAAAgAAAAAAAAAAAAAA" +
        "AAAAAAMAAAAAAAAABAAAAAAAAACAAYa+AAAAAA=="
    )
  }

  it should "serialize inner decimal values" in {
    val schema = StructType(Seq(
      StructField("rec", StructType(Seq(
        StructField("key", StringType),
        StructField("value", DecimalType(9, 4))
      )))
    ))

    val data = Seq(
      Row(Row("key1", java.math.BigDecimal.valueOf(10.01))),
      Row(Row("key2", java.math.BigDecimal.valueOf(10.002))),
      Row(Row("key3", java.math.BigDecimal.valueOf(10.0003))),
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val resultBase64 = GenericRowSerializer.dfToYTFormatWithBase64(df)
    resultBase64 should contain theSameElementsAs Seq(
      "NQAAAAAAAAAKLwoDcmVjEBFIAFIkEiIiIAoLCgNrZXkSBBICCBAKEQoFdmFsdWUSCBIGUgQICRAEEAEYAAAAAAMAAAAAAAAAAQAAAAAAA" +
        "AAAAAAAAAAAAA8AAAAAAAAAWwEIa2V5MTsBCIABhwRdAAEAAAAAAAAAAAAAAAAAAAAPAAAAAAAAAFsBCGtleTI7AQiAAYa0XQABAAAA" +
        "AAAAAAAAAAAAAAAADwAAAAAAAABbAQhrZXkzOwEIgAGGo10A"
    )
  }
}
