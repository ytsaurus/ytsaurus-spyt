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

  private val rowCountLimit = 10

  it should "serialize dataframe to base64" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res, rowCountLimit)
    val answer = Array(
      "F",
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

    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res, rowCountLimit)
    val answer = Array(
      "F",
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

    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res, rowCountLimit)
    val answer = Array(
      "F",
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
    val resultBase64 = GenericRowSerializer.dfToYTFormatWithBase64(res, rowCountLimit)

    resultBase64 should contain theSameElementsAs Seq(
      "F",
      "KwAAAAAAAAAKEAoCaWQQA0ADSABSBBICCAMKEwoFdmFsdWUQEEAQSABSBBICCBAQARgAAAAAAAACAAAAAAAAAAIAAAAAAAAAAAAAAAAA" +
        "AAABAAAAAAAAABMAAAAAAAAA0J3QvtC80LXRgCDQvtC00LjQvQAAAAAAAgAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAEQAAAAAAAADQnd" +
        "C+0LzQtdGAINC00LLQsAAAAAAAAAA="
    )
  }

  it should "serialize decimal values" in {
    import spark.implicits._

    val df = spark.createDataset(Seq((1L, 10.0), (2L, 10.2), (3L, 10.003)))
      .toDF().select($"_1".as("id"), $"_2".cast(DecimalType(9, 4)).as("value"))

    val resultBase64 = GenericRowSerializer.dfToYTFormatWithBase64(df, rowCountLimit)
    resultBase64 should contain theSameElementsAs Seq(
      "F",
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

    val resultBase64 = GenericRowSerializer.dfToYTFormatWithBase64(df, rowCountLimit)
    resultBase64 should contain theSameElementsAs Seq(
      "F",
      "NQAAAAAAAAAKLwoDcmVjEBFIAFIkEiIiIAoLCgNrZXkSBBICCBAKEQoFdmFsdWUSCBIGUgQICRAEEAEYAAAAAAMAAAAAAAAAAQAAAAAAA" +
        "AAAAAAAAAAAAA8AAAAAAAAAWwEIa2V5MTsBCIABhwRdAAEAAAAAAAAAAAAAAAAAAAAPAAAAAAAAAFsBCGtleTI7AQiAAYa0XQABAAAA" +
        "AAAAAAAAAAAAAAAADwAAAAAAAABbAQhrZXkzOwEIgAGGo10A"
    )
  }

  it should "truncate result set up to rowCountLimit" in {
    writeTableFromYson((1 to 15).map {id =>
      s"""{a = $id; b = "Value $id"; c = $id.$id}"""
    }, tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res, rowCountLimit)
    val answer = Array(
      "T",
      "NwAAAAAAAAAKDwoBYRADQANIAFIEEgIIAwoPCgFiEBBAEEgAUgQSAggQCg8KAWMQBUAFSABSBBICCAUQARgAAAoAAAAAAAAAAwAAAAAAA" +
        "AAAAAAAAAAAAAEAAAAAAAAABwAAAAAAAABWYWx1ZSAxAJqZmZmZmfE/AwAAAAAAAAAAAAAAAAAAAAIAAAAAAAAABwAAAAAAAABWYWx1" +
        "ZSAyAJqZmZmZmQFAAwAAAAAAAAAAAAAAAAAAAAMAAAAAAAAABwAAAAAAAABWYWx1ZSAzAGZmZmZmZgpAAwAAAAAAAAAAAAAAAAAAAAQ" +
        "AAAAAAAAABwAAAAAAAABWYWx1ZSA0AJqZmZmZmRFAAwAAAAAAAAAAAAAAAAAAAAUAAAAAAAAABwAAAAAAAABWYWx1ZSA1AAAAAAAAAB" +
        "ZAAwAAAAAAAAAAAAAAAAAAAAYAAAAAAAAABwAAAAAAAABWYWx1ZSA2AGZmZmZmZhpAAwAAAAAAAAAAAAAAAAAAAAcAAAAAAAAABwAAA" +
        "AAAAABWYWx1ZSA3AM3MzMzMzB5AAwAAAAAAAAAAAAAAAAAAAAgAAAAAAAAABwAAAAAAAABWYWx1ZSA4AJqZmZmZmSFAAwAAAAAAAAAA" +
        "AAAAAAAAAAkAAAAAAAAABwAAAAAAAABWYWx1ZSA5AM3MzMzMzCNAAwAAAAAAAAAAAAAAAAAAAAoAAAAAAAAACAAAAAAAAABWYWx1ZSA" +
        "xMDMzMzMzMyRA"
    )
    tableBytes should contain theSameElementsAs answer
  }
}
