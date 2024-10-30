package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.functions.element_at
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.sql.{DataFrameReader, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.common.Decimal.textToBinary
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.TypeV3
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.format.types.ComplexTypeV3Test._
import tech.ytsaurus.spyt.format.{Data, Info}
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.typeinfo.StructType.Member
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.yson.YsonParser
import tech.ytsaurus.ysontree.{YTree, YTreeBuilder}

import scala.collection.mutable.ListBuffer

class ComplexTypeV3Test extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.yt.schema.forcingNullableIfNoMetadata.enabled", value = false)
  }

  override def afterAll(): Unit = {
    spark.conf.set("spark.yt.schema.forcingNullableIfNoMetadata.enabled", value = true)
    super.afterAll()
  }

  // TODO put in TestUtils
  private def testEnabledAndDisabledArrow(f: DataFrameReader => Unit): Unit = {
    f(spark.read.enableArrow)
    f(spark.read.disableArrow)
  }


  it should "read optional from yt" in {
    forAllWriteProtocols {
      val data = Seq(Some(1L), Some(2L), None)

      writeTableFromYson(
        data.map(
          d => s"""{ optional = ${d.map(_.toString).getOrElse("#")} }"""),
        YtWrapper.removeIfExists(tmpPath), TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("optional", TiType.optional(TiType.int64()))
          .build()
      )

      val res = spark.read.yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(x => Row(x.orNull))
    }
  }

  it should "read decimal from yt" in {
    forAllWriteProtocols {
      val precision = 3
      val scale = 2
      val data = Seq("1.23", "0.21")
      val byteDecimal = data.map(x => textToBinary(x, precision, scale))
      writeTableFromURow(byteDecimal.map(x => packToRow(x, ColumnValueType.STRING)), YtWrapper.removeIfExists(tmpPath),
        TableSchema.builder().setUniqueKeys(false).addValue("a", TiType.decimal(precision, scale)).build())

      withConf(s"spark.yt.${TypeV3.name}", "true") {
        testEnabledAndDisabledArrow { reader =>
          val res = reader.yt(tmpPath)
          res.collect().map(x => x.getDecimal(0).toString) should contain theSameElementsAs data
        }
      }
    }
  }

  it should "read array from yt" in {
    forAllWriteProtocols {
      val data = Seq(Seq(1L, 2L), Seq(3L, 4L, 5L))
      writeTableFromURow(
        data.map(x => packToRow(codeList(x))), YtWrapper.removeIfExists(tmpPath), TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("array", TiType.list(TiType.int64()))
          .build()
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(Row(_))
    }
  }

  it should "read map from yt" in {
    forAllWriteProtocols {
      val data = Seq(Map("1" -> true), Map("3" -> true, "4" -> false))
      writeTableFromURow(
        data.map(x => packToRow(codeDictLikeList(x))), YtWrapper.removeIfExists(tmpPath), TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("map", TiType.dict(TiType.string(), TiType.bool()))
          .build()
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(Row(_))
    }
  }

  it should "read struct from yt" in {
    forAllWriteProtocols {
      val schema = TableSchema.builder()
        .setUniqueKeys(false)
        .addValue("struct",
          TiType.struct(
            new Member("d", TiType.doubleType()),
            new Member("s", TiType.string())
          ))
        .build()
      val data = Seq(TestStruct(0.2, "ab"), TestStruct(0.9, "d"))

      writeTableFromURow(
        data.map(x => packToRow(codeTestStruct(x))), YtWrapper.removeIfExists(tmpPath), schema
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(x => Row(Row(x.d, x.s)))
    }
  }

  it should "read complex struct with decimals from yt and access inner fields" in {
    forAllWriteProtocols {
      val schema = TableSchema.builder()
        .setUniqueKeys(false)
        .addValue("limits", TiType.optional(
          TiType.struct(
            new Member("lower_limit", TiType.optional(TiType.decimal(35, 18))),
            new Member("upper_limit", TiType.optional(TiType.decimal(35, 18)))
          )))
        .build()

      def buildRow(lowerLimit: String, upperLimit: String): Array[Byte] = YTree.listBuilder()
        .value(textToBinary(lowerLimit, 35, 18))
        .value(textToBinary(upperLimit, 35, 18))
        .endList().build().toBinary

      val sampleData = Seq(
        packToRow(buildRow("0.05", "3.05")),
        packToRow(buildRow("0.70", "9.00")),
        packToRow(buildRow("0.05", "3.05")),
        packToRow(buildRow("0.70", "9.00"))
      )

      writeTableFromURow(sampleData, YtWrapper.removeIfExists(tmpPath), schema)

      val df = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)


      val twoColResult = df.select($"limits.upper_limit" - $"limits.lower_limit").as[BigDecimal].collect()
      twoColResult should contain theSameElementsInOrderAs List(3.0, 8.3, 3.0, 8.3).map(BigDecimal(_))

      val leftColResult = df.select($"limits.upper_limit").as[BigDecimal].collect()
      leftColResult should contain theSameElementsInOrderAs List(3.05, 9.00, 3.05, 9.00).map(BigDecimal(_))

      val rightColResult = df.select($"limits.lower_limit").as[BigDecimal].collect()
      rightColResult should contain theSameElementsInOrderAs List(0.05, 0.70, 0.05, 0.70).map(BigDecimal(_))
    }
  }

  it should "read tuple from yt" in {
    forAllWriteProtocols {
      val data: Seq[Array[Any]] = Seq(Array[Any](99L, 0.3), Array[Any](128L, 1.0))
      writeTableFromURow(
        data.map { x => packToRow(codeList(x)) }, YtWrapper.removeIfExists(tmpPath), TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("tuple",
            TiType.tuple(
              TiType.int64(),
              TiType.doubleType()
            ))
          .build()
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(x => Row(Row(x: _*)))
    }
  }

  it should "read tagged from yt" in {
    forAllWriteProtocols {
      writeTableFromYson(
        Seq("{ tagged = 1 }", "{ tagged = 2 }"), YtWrapper.removeIfExists(tmpPath),
        TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("tagged",
            TiType.tagged(
              TiType.int64(),
              "main"
            ))
          .build()
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.schema shouldBe StructType(Seq(
        StructField("tagged", LongType, nullable = false,
          getMetadataBuilder("tagged").putString(MetadataFields.TAG, "main").build())
      ))
      res.collect() should contain theSameElementsAs Seq(Row(1L), Row(2L))
    }
  }

  it should "read variant over tuple from yt" in {
    forAllWriteProtocols {
      val data: Seq[Seq[Any]] = Seq(Seq(null, 0.3), Seq("s", null))
      writeTableFromURow(
        Seq(packToRow(codeList(Array[Any](1L, data(0)(1))), ColumnValueType.COMPOSITE),
          packToRow(codeList(Array[Any](0L, data(1)(0))), ColumnValueType.COMPOSITE)),
        YtWrapper.removeIfExists(tmpPath), TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("variant",
            TiType.variantOverTuple(
              TiType.string(),
              TiType.doubleType()
            ))
          .build()
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(x => Row(Row(x: _*)))
    }
  }

  it should "read variant over struct with positional view from yt" in {
    forAllWriteProtocols {
      val data: Seq[Seq[Any]] = Seq(Seq(null, 0.3), Seq("t", null))
      writeTableFromURow(
        Seq(packToRow(codeList(Array[Any](1L, data(0)(1))), ColumnValueType.COMPOSITE),
          packToRow(codeList(Array[Any](0L, data(1)(0))), ColumnValueType.COMPOSITE)),
        YtWrapper.removeIfExists(tmpPath), TableSchema.builder()
          .setUniqueKeys(false)
          .addValue("variant",
            TiType.variantOverStruct(java.util.List.of(
              new Member("s", TiType.string()),
              new Member("d", TiType.doubleType())
            )))
          .build()
      )

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs data.map(x => Row(Row(x: _*)))
    }
  }

  it should "read nested float values" in {
    forAllWriteProtocols {
      val tableSchema = TableSchema.builder()
        .addValue("id", TiType.int64())
        .addValue("plain_value_f", TiType.floatType())
        .addValue("plain_value_d", TiType.doubleType())
        .addValue("nested_value_f", TiType.dict(TiType.utf8(), TiType.floatType()))
        .addValue("nested_value_d", TiType.dict(TiType.utf8(), TiType.doubleType()))
        .addValue("list_f", TiType.list(TiType.floatType()))
        .addValue("opt_f", TiType.optional(TiType.floatType()))
        .addValue("struct_f", TiType.struct(
          TiType.member("key", TiType.string()), TiType.member("f_value", TiType.floatType())
        ))
        .addValue("tuple_f", TiType.tuple(TiType.floatType(), TiType.floatType()))
        .build()

      def createRow(id: Long, pvF: Float, pvD: Double, nvF: Map[String, Float], nvD: Map[String, Double],
                    listF: List[Float], optF: Option[Float], structF: (String, Float), tupleF: (Float, Float)) = {
        val nvFyson = nvF.foldLeft(YTree.listBuilder()) { case (builder, (key, floatValue)) =>
          builder.value(YTree.listBuilder().value(key).value(floatValue).endList().build())
        }.endList().build()

        val nvDyson = nvD.foldLeft(YTree.listBuilder()) { case (builder, (key, doubleValue)) =>
          builder.value(YTree.listBuilder().value(key).value(doubleValue).endList().build())
        }.endList().build()

        val listFyson = listF.foldLeft(YTree.listBuilder())((builder, fValue) => builder.value(fValue)).endList().build()
        val structFyson = YTree.listBuilder().value(structF._1).value(structF._2).endList().build()
        val tupleFYson = YTree.listBuilder().value(tupleF._1).value(tupleF._2).endList().build()

        new UnversionedRow(java.util.List.of[UnversionedValue](
          new UnversionedValue(0, ColumnValueType.INT64, false, id),
          new UnversionedValue(1, ColumnValueType.DOUBLE, false, pvF.toDouble),
          new UnversionedValue(2, ColumnValueType.DOUBLE, false, pvD),
          new UnversionedValue(3, ColumnValueType.COMPOSITE, false, nvFyson.toBinary),
          new UnversionedValue(4, ColumnValueType.COMPOSITE, false, nvDyson.toBinary),
          new UnversionedValue(5, ColumnValueType.COMPOSITE, false, listFyson.toBinary),
          new UnversionedValue(6, if (optF.isDefined) ColumnValueType.DOUBLE else ColumnValueType.NULL, false,
            optF.map(_.toDouble).orNull),
          new UnversionedValue(7, ColumnValueType.COMPOSITE, false, structFyson.toBinary),
          new UnversionedValue(8, ColumnValueType.COMPOSITE, false, tupleFYson.toBinary)
        ))
      }

      writeTableFromURow(Seq(
        createRow(1, 1.1f, 1.3, Map("key1" -> 2.3f, "key2" -> 4.5f), Map("key3" -> 6.7, "key4" -> 8.9), List(1.2f),
          Some(4.5f), "some_key" -> 6.7f, (8.1f, 9.2f)),
        createRow(2, 2.2f, 2.4, Map("key1" -> 2.4f, "key2" -> 4.6f), Map("key3" -> 6.8, "key4" -> 8.8), List(3.4f, 5.6f),
          Some(6.7f), "some_key" -> 7.7f, (6.1f, 7.4f)),
        createRow(3, 3.3f, 3.5, Map("key1" -> 2.5f, "key2" -> 4.7f), Map("key3" -> 6.9, "key4" -> 8.7),
          List(7.8f, 9.1f, 2.3f), None, "some_key" -> 8.7f, (5.3f, 8.2f))
      ), YtWrapper.removeIfExists(tmpPath), tableSchema)

      val df = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsAs Seq(
        StructField("id", LongType, nullable = false),
        StructField("plain_value_f", FloatType, nullable = false),
        StructField("plain_value_d", DoubleType, nullable = false),
        StructField("nested_value_f", MapType(StringType, DoubleType, valueContainsNull = false), nullable = false),
        StructField("nested_value_d", MapType(StringType, DoubleType, valueContainsNull = false), nullable = false),
        StructField("list_f", ArrayType(DoubleType, containsNull = false), nullable = false),
        StructField("opt_f", FloatType, nullable = true),
        StructField("struct_f", StructType(Seq(
          StructField("key", StringType, nullable = false),
          StructField("f_value", DoubleType, nullable = false)
        )), nullable = false),
        StructField("tuple_f", StructType(Seq(
          StructField("_1", DoubleType, nullable = false),
          StructField("_2", DoubleType, nullable = false)
        )), nullable = false)
      )

      import spark.implicits._
      val resultDf = df.select(
        $"plain_value_f",
        $"plain_value_d".cast(FloatType),
        $"nested_value_d".getField("key3").cast(FloatType),
        $"nested_value_f".getField("key1").cast(FloatType),
        element_at($"list_f", 1).cast(FloatType),
        $"opt_f".cast(FloatType),
        $"struct_f.f_value".cast(FloatType),
        $"tuple_f._1".cast(FloatType),
        $"tuple_f._2".cast(FloatType)
      )

      val result = resultDf.collect()

      result should contain theSameElementsAs Seq(
        Row(1.1f, 1.3f, 6.7f, 2.3f, 1.2f, 4.5f, 6.7f, 8.1f, 9.2f),
        Row(2.2f, 2.4f, 6.8f, 2.4f, 3.4f, 6.7f, 7.7f, 6.1f, 7.4f),
        Row(3.3f, 3.5f, 6.9f, 2.5f, 7.8f, null, 8.7f, 5.3f, 8.2f)
      )
    }  }

  it should "write decimal to yt" in {
    forAllWriteProtocols {
      import spark.implicits._
      val data = Seq(BigDecimal("1.23"), BigDecimal("0.21"), BigDecimal("0"), BigDecimal("0.1"))
      data
        .toDF("a").coalesce(1)
        .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      testEnabledAndDisabledArrow { reader =>
        val res = reader.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

        res.columns should contain theSameElementsAs Seq("a")
        res.collect().map(x => x.getDecimal(0).toPlainString) should contain theSameElementsAs Seq(
          "1.230000000000000",
          "0.210000000000000",
          "0.000000000000000",
          "0.100000000000000"
        )
      }
    }
  }

  it should "write array to yt" in {
    forAllWriteProtocols {
      import spark.implicits._
      val data = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
      data
        .toDF("a").coalesce(1)
        .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).mode("overwrite").yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a")
      res.select("a").collect() should contain theSameElementsAs data.map(Row(_))
    }
  }

  it should "write array of decimals to yt" in {
    import spark.implicits._
    val data = Seq(Seq(BigDecimal("1.23")), Seq(BigDecimal("0.21"), BigDecimal("0")))
    data
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

    testEnabledAndDisabledArrow { reader =>
      val res = reader.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      val resLists = res.as[Seq[java.math.BigDecimal]].collect().map(_.map(_.toPlainString))

      resLists should contain theSameElementsAs Seq(
        Seq("1.230000000000000"), Seq("0.210000000000000", "0.000000000000000")
      )
    }
  }

  it should "write map to yt" in {
    forAllWriteProtocols {
      import spark.implicits._
      val data = Seq(
        Map("spark" -> Map(0 -> 3.14, 1 -> 2.71), "over" -> Map(2 -> -1.0)),
        Map("yt" -> Map(5 -> 5.0)))
      data
        .toDF("a").coalesce(1)
        .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a")
      res.collect() should contain theSameElementsAs data.map(Row(_))
    }
  }

  it should "write array of maps to yt" in {
    import spark.implicits._
    val data = Seq(
      Seq(Map("spark" -> BigDecimal("1.23"), "over" -> BigDecimal("1.21"))),
      Seq(Map("yt" -> BigDecimal("0.123456")), Map("spyt" -> BigDecimal("0.7")))
    )
    data
      .toDF("a").coalesce(1)
      .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

    testEnabledAndDisabledArrow { reader =>
      val res = reader.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      val resLists = res.as[Seq[Map[String, java.math.BigDecimal]]].collect().map(_.map(_.mapValues(_.toPlainString)))

      resLists should contain theSameElementsAs Seq(
        Seq(Map("spark" -> "1.230000000000000", "over" -> "1.210000000000000")),
        Seq(Map("yt" -> "0.123456000000000"), Map("spyt" -> "0.700000000000000"))
      )
    }
  }

  it should "write tagged from yt" in {
    forAllWriteProtocols {
      val data = Seq(1L, 2L)
      data.map(Some(_))
        .toDF("tagged").coalesce(1).write
        .schemaHint(Map("tagged" -> YtLogicalType.Tagged(YtLogicalType.Int64, "main")))
        .option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.schema shouldBe StructType(Seq(
        StructField("tagged", LongType, nullable = false,
          getMetadataBuilder("tagged").putString(MetadataFields.TAG, "main").build())
      ))
      res.collect() should contain theSameElementsAs Seq(Row(1L), Row(2L))
    }
  }

  it should "write struct to yt" in {
    forAllWriteProtocols {
      import spark.implicits._
      val data = Seq(TestStruct(1.0, "a"), TestStruct(3.2, "b"))
      data.map(Some(_))
        .toDF("a").coalesce(1)
        .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a")
      res.collect() should contain theSameElementsAs data.map(x => Row(Row.fromTuple(x)))
    }
  }

  it should "write tuple to yt" in {
    forAllWriteProtocols {
      import spark.implicits._
      val data = Seq((1, "a"), (3, "c"))
      data.map(Some(_))
        .toDF("a").coalesce(1)
        .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a")
      res.collect() should contain theSameElementsAs data.map(x => Row(Row.fromTuple(x)))
    }
  }

  it should "write variant over tuple from yt" in {
    forAllWriteProtocols {
      val data = Seq(Tuple2(Some("1"), None), Tuple2(None, Some(2.0)))
      val nullableData = Seq(Tuple2("1", null), Tuple2(null, 2.0))
      data.map(Some(_))
        .toDF("a").coalesce(1).write
        .schemaHint(Map("a" ->
          YtLogicalType.VariantOverTuple(Seq(
            (YtLogicalType.String, Metadata.empty), (YtLogicalType.Double, Metadata.empty)))))
        .option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs nullableData.map(x => Row(Row.fromTuple(x)))
    }
  }

  it should "write variant over struct with positional view from yt" in {
    forAllWriteProtocols {
      val data = Seq(TestVariant(None, Some("2.0")), TestVariant(Some(1), None))
      val nullableData = Seq(Tuple2(null, "2.0"), Tuple2(1, null))
      data.map(Some(_))
        .toDF("a").coalesce(1).write
        .schemaHint(Map("a" ->
          YtLogicalType.VariantOverStruct(Seq(
            ("i", YtLogicalType.Int32, Metadata.empty), ("s", YtLogicalType.String, Metadata.empty)))))
        .option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.collect() should contain theSameElementsAs nullableData.map(x => Row(Row.fromTuple(x)))
    }
  }

  it should "not change variant schema in read-write operation" in {
    forAllWriteProtocols {
      val tmpPath2 = s"$tmpPath-copy"
      val data = Seq(TestVariant(None, Some("2.0")), TestVariant(Some(1), None))
      data.map(Some(_))
        .toDF("a").coalesce(1).write
        .schemaHint(Map("a" ->
          YtLogicalType.VariantOverStruct(Seq(
            ("i", YtLogicalType.Int32, Metadata.empty), ("s", YtLogicalType.String, Metadata.empty)))))
        .option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)
      res.write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath2))
      val res2 = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath2)

      res.schema shouldBe res2.schema
      res2.schema shouldBe StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("_vi", IntegerType, nullable = true,
            metadata = new MetadataBuilder().putBoolean("optional", false).build()),
          StructField("_vs", StringType, nullable = true,
            metadata = new MetadataBuilder().putBoolean("optional", false).build())
        )), nullable = false,
          metadata = getMetadataBuilder("a").build())
      ))
    }
  }

  it should "write combined complex types" in {
    forAllWriteProtocols {
      val data = Seq(
        (Map(1 -> TestStructHard(2, Some(Seq(TestStruct(3.0, "4"), TestStruct(5.0, "6"))))), "a"),
        (Map(7 -> TestStructHard(0, None)), "b")
      )
      data.map(Some(_))
        .toDF("a").coalesce(1)
        .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

      val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a")
      res.collect() should contain theSameElementsAs Seq(
        Row(Row(Map(1 -> Row(2, Seq(Row(3.0, "4"), Row(5.0, "6")))), "a")),
        Row(Row(Map(7 -> Row(0, null)), "b"))
      )
    }
  }

  it should "generate nullable correct schema" in {
    forAllWriteProtocols {
      val data = Seq(
        (Map(1 -> TestStructHard(2, Some(Seq(TestStruct(3.0, "4"), TestStruct(5.0, "6"))))), "a"),
        (Map(7 -> TestStructHard(0, None)), "b")
      )
      withConf("spark.yt.schema.forcingNullableIfNoMetadata.enabled", "false") {
        data.map(Some(_))
          .toDF("a").coalesce(1)
          .write.option(YtTableSparkSettings.WriteTypeV3.name, value = true).yt(YtWrapper.removeIfExists(tmpPath))

        val res = spark.read.option(YtUtils.Options.PARSING_TYPE_V3, value = true).yt(tmpPath)

        res.schema shouldBe StructType(Seq(
          StructField("a",
            StructType(Seq(
              StructField("_1", MapType(IntegerType,
                StructType(Seq(
                  StructField("v", IntegerType, nullable = false),
                  StructField("l", ArrayType(
                    StructType(Seq(
                      StructField("d", DoubleType, nullable = false),
                      StructField("s", StringType, nullable = true))),
                    containsNull = true), nullable = true))),
                valueContainsNull = true), nullable = true),
              StructField("_2", StringType, nullable = true))),
            nullable = true,
            metadata = getMetadataBuilder("a").build())))
      }
    }
  }

  it should "write dataset with struct with float" in {
    import spark.implicits._

    List(Info("Test", Data(32, 100.0f)), Info("Test2", Data(30, 99.9f)))
      .toDF("name", "data")
      .write
      .option(YtTableSparkSettings.WriteTypeV3.name, value = true)
      .yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))

    schema shouldEqual TableSchema.builder().setUniqueKeys(false)
      .addValue("name", TiType.optional(TiType.string()))
      .addValue("data",  TiType.optional(TiType.struct(
        new Member("age", TiType.int32()),
        new Member("weight", TiType.doubleType())
      )))
      .build()

    val readData = readTableAsYson(tmpPath).map { row =>
      val map = row.asMap()
      val dataBytes = map.get("data").bytesValue()
      val ysonParser: YsonParser = new YsonParser(dataBytes)
      val ytreeBuilder: YTreeBuilder = YTree.builder()
      ysonParser.parseNode(ytreeBuilder)

      val dataMap = ytreeBuilder.build().asList()
      (
        map.get("name").stringValue(),
        dataMap.get(0).intValue(),
        dataMap.get(1).floatValue()
      )
    }

    readData should contain theSameElementsAs ListBuffer[(String, Int, Double)](
      ("Test", 32, 100.0f),
      ("Test2", 30, 99.9f),
    )
  }

  it should "read dataset with struct with float" in {
    val data: ListBuffer[UnversionedRow] = ListBuffer(
      packToRow(
        "Test", YTree.listBuilder().value(32).value(100.0f).endList().build().toBinary
      ),
      packToRow(
        "Test2", YTree.listBuilder().value(30).value(99.9f).endList().build().toBinary
      ),
    )

    val schema: TableSchema = TableSchema.builder().setUniqueKeys(false)
      .addValue("name", TiType.optional(TiType.string()))
      .addValue("data",  TiType.optional(TiType.struct(
        new Member("age", TiType.int32()),
        new Member("weight", TiType.floatType())
      )))
      .build()

    writeTableFromURow(data, tmpPath, schema)

    val df = spark.read
      .option(YtUtils.Options.PARSING_TYPE_V3, value = true)
      .schemaHint(
        "name" -> StringType,
        "data" -> StructType(Seq(
          StructField("age", IntegerType),
          StructField("weight", FloatType)))
      )
      .yt(tmpPath).as[Info]

    df.collect() should contain theSameElementsAs Seq(
      Info("Test", Data(32, 100.0f)),
      Info("Test2", Data(30, 99.9f))
    )
  }

  it should "write long strings" in {
    withSparkConfArrowWrite(enabled = true) {
      val string = "1" * 20000000
      val data = Seq(string)
      data.map(Some(_))
        .toDF("a").coalesce(1)
        .write.optimizeFor("scan")
        .option(YtTableSparkSettings.WriteTableConfig, YTree.builder.beginMap().key("max_row_weight").value(30000000L).endMap().build())
        .option(YtTableSparkSettings.WriteTypeV3.name, value = true)
        .yt(YtWrapper.removeIfExists(tmpPath))
      spark.read
        .enableArrow
        .option(YtUtils.Options.PARSING_TYPE_V3, value = true)
        .yt(tmpPath).collect()(0)(0) shouldBe string
    }
  }
}

object ComplexTypeV3Test {
  private def codeListImpl(list: Seq[Any], transformer: (YTreeBuilder, Int, Any) => Unit): Array[Byte] = {
    val builder = new YTreeBuilder
    builder.onBeginList()
    list.zipWithIndex.foreach {
      case (value, index) =>
        builder.onListItem()
        transformer(builder, index, value)
    }
    builder.onEndList()
    builder.build.toBinary
  }

  private def codeDictImpl(map: Map[String, Any]): Array[Byte] = {
    val builder = new YTreeBuilder
    builder.onBeginMap()
    map.foreach {
      case (key, value) =>
        builder.key(key)
        builder.value(value)
    }
    builder.onEndMap()
    builder.build.toBinary
  }

  private def codeList(list: Seq[Any]): Array[Byte] = {
    codeListImpl(list,
      (builder, _, value) =>
        builder.value(value)
    )
  }

  private def codeUList(cVType: ColumnValueType, list: Seq[Any]): Array[Byte] = {
    codeListImpl(list,
      (builder, index, value) =>
        new UnversionedValue(index, cVType, false, value)
          .writeTo(builder)
    )
  }

  private def codeDictLikeList[T](map: Map[T, Any]): Array[Byte] = {
    codeUList(ColumnValueType.COMPOSITE, map.map { case (a, b) => codeList(Seq(a, b)) }.toSeq)
  }

  private def codeTestStruct(struct: TestStruct): Array[Byte] = {
    codeList(Seq[Any](struct.d, struct.s))
  }

  private def getMetadataBuilder(name: String, keyId: Int = -1): MetadataBuilder = {
    new MetadataBuilder()
      .putString(MetadataFields.ORIGINAL_NAME, name)
      .putLong(MetadataFields.KEY_ID, keyId)
      .putBoolean(MetadataFields.ARROW_SUPPORTED, true)
  }


  private def packToRow(value: Any,
                        cVType: ColumnValueType = ColumnValueType.COMPOSITE): UnversionedRow = {
    new UnversionedRow(java.util.List.of[UnversionedValue](
      new UnversionedValue(0, cVType, false, value)
    ))
  }

  def packToRow(value1: String, value2: Array[Byte]): UnversionedRow = {
    new UnversionedRow(java.util.List.of[UnversionedValue](
      new UnversionedValue(0, ColumnValueType.STRING, false, value1.getBytes),
      new UnversionedValue(1, ColumnValueType.COMPOSITE, false, value2)
    ))
  }
}

case class TestStruct(d: Double, s: String)

case class TestVariant(i: Option[Int], s: Option[String])

case class TestStructHard(v: Int, l: Option[Seq[TestStruct]])

