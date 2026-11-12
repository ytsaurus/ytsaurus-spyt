package tech.ytsaurus.spyt.format.batch

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnVector
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.SparkAdapter
import tech.ytsaurus.spyt.YsonAsVariantTestUtils.whenVariantSupported
import tech.ytsaurus.spyt.types.YsonVariantCodec
import tech.ytsaurus.spyt.wrapper.table.YtArrowInputStream

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

class ArrowVariantBatchReaderTest extends AnyFlatSpec with Matchers {

  private val cells = Seq(
    "18446744073709551615u", "{a=[1;#]}", "#", "<a=1>#", null)
  private val tableSchema = TableSchema.builder().addValue("value", ColumnValueType.ANY).build()

  Seq(false, true).foreach { dictionary =>
    it should s"read Variant values and nulls across Arrow batches and pages, dictionary=$dictionary" in {
      whenVariantSupported {
        val converter = SparkAdapter.instance.ysonVariantConverter
        val schema = StructType(Seq(StructField("value", converter.variantType)))
        val batches = Seq(cells.indices.reverse ++ Seq(1, 0, -1), Seq.empty, Seq(2, 1, 0, -1), Seq(3, 0))
        val reader = new ArrowBatchReader(input(cells, batches, dictionary), schema, tableSchema)
        try {
          var previous: Option[ColumnVector] = None
          batches.filter(_.nonEmpty).foreach { indices =>
            reader.nextBatch shouldBe true
            previous.foreach(assertReleased)
            val batch = reader.currentBatch
            previous = Some(batch.column(0))
            val expected = indices.map { index =>
              if (index < 0 || cells(index) == null) null
              else YsonVariantCodec.fromYson(converter, cells(index).getBytes(UTF_8), false)
            }
            batch.numRows() shouldBe indices.size
            batch.column(0).numNulls() shouldBe expected.count(_ == null)
            expected.indices.foreach { row =>
              batch.column(0).isNullAt(row) shouldBe (expected(row) == null)
              batch.getRow(row).get(0, converter.variantType) shouldBe expected(row)
            }
          }
          reader.nextBatch shouldBe false
          previous.foreach(assertReleased)
        } finally {
          reader.close()
        }
      }
    }
  }

  it should "return null for a missing Variant column" in {
    whenVariantSupported {
      val variantType = SparkAdapter.instance.ysonVariantConverter.variantType
      val schema = StructType(Seq(StructField("missing", variantType)))
      val reader = new ArrowBatchReader(input(cells, Seq(Seq(0, 1)), false), schema, tableSchema)
      try {
        reader.currentBatch.numCols() shouldBe 1
        reader.nextBatch shouldBe true
        reader.currentBatch.column(0).numNulls() shouldBe 2
        reader.currentBatch.getRow(0).get(0, variantType) shouldBe null
        val vector = reader.currentBatch.column(0)
        reader.close()
        assertReleased(vector)
      } finally {
        reader.close()
      }
    }
  }

  Seq(false, true).foreach { allowDuplicates =>
    it should s"capture the duplicate-key policy when creating the reader, allowDuplicates=$allowDuplicates" in {
      whenVariantSupported {
        val conf = new SQLConf
        val key = "spark.sql.variant.allowDuplicateKeys"
        conf.setConfString(key, allowDuplicates.toString)
        SQLConf.withExistingConf(conf) {
          val converter = SparkAdapter.instance.ysonVariantConverter
          val schema = StructType(Seq(StructField("value", converter.variantType)))
          val reader = new ArrowBatchReader(input(Seq("{a=1;a=2}"), Seq(Seq(0)), false), schema, tableSchema)
          conf.setConfString(key, (!allowDuplicates).toString)
          try {
            if (allowDuplicates) {
              reader.nextBatch shouldBe true
              reader.currentBatch.getRow(0).get(0, converter.variantType) shouldBe
                YsonVariantCodec.fromYson(converter, "{a=2}".getBytes(UTF_8), true)
            } else {
              val expected = intercept[RuntimeException] {
                YsonVariantCodec.fromYsonColumn(converter, "{a=1;a=2}".getBytes(UTF_8), false, "value")
              }
              val actual = intercept[RuntimeException](reader.nextBatch)
              actual.getClass shouldBe expected.getClass
              actual.getMessage shouldBe expected.getMessage
            }
          } finally {
            reader.close()
          }
        }
      }
    }
  }

  it should "report an invalid Variant key with the same column context as wire" in {
    whenVariantSupported {
      val converter = SparkAdapter.instance.ysonVariantConverter
      val schema = StructType(Seq(StructField("value", converter.variantType)))
      val value = "{\"\\xFF\"=1}"
      var closed = false
      val reader = new ArrowBatchReader(input(Seq("{a=1}", value), Seq(Seq(0, 1)), false), schema, tableSchema) {
        override def close(): Unit = {
          super.close()
          closed = true
        }
      }
      try {
        val actual = intercept[IllegalArgumentException](reader.nextBatch)
        val invalidKey = "YSON key \"\\xFF\" is not valid UTF-8"
        actual.getMessage should include(s"Column value cannot be read as Variant: $invalidKey")
        actual.getCause shouldBe a[IllegalArgumentException]
        actual.getCause.getMessage shouldBe s"$invalidKey, so it cannot be a Variant object key."
        closed shouldBe true
      } finally {
        reader.close()
      }
    }
  }

  private def assertReleased(vector: ColumnVector): Unit = {
    // Spark clears the child vector array when closing a Variant vector.
    an[NullPointerException] should be thrownBy vector.getChild(0)
  }

  private def input(values: Seq[String], batches: Seq[Seq[Int]], dictionary: Boolean): YtArrowInputStream = {
    val pages = Seq(batches.take(1), batches.drop(1)).filter(_.nonEmpty).zipWithIndex.map { case (page, index) =>
      // Change dictionary ids on the second page to catch stale dictionary reuse.
      val reversed = dictionary && index > 0
      val pageValues = if (reversed) values.reverse else values
      val pageBatches = if (reversed) page.map(_.map(i => if (i < 0) i else values.size - 1 - i)) else page
      new ByteArrayInputStream(arrowBytes(pageValues, pageBatches, dictionary))
    }
    new YtArrowInputStream {
      private var page = 0
      override def isNextPage: Boolean = {
        // Each page ends with the 8-byte Arrow stream terminator.
        if (pages(page).available() == 8 && page + 1 < pages.size) {
          page += 1
          true
        } else false
      }
      override def isEmptyPage: Boolean = false
      override def read(): Int = pages(page).read()
      override def read(buffer: Array[Byte], offset: Int, length: Int): Int = pages(page).read(buffer, offset, length)
    }
  }

  private def arrowBytes(values: Seq[String], batches: Seq[Seq[Int]], dictionary: Boolean): Array[Byte] = {
    val allocator = new RootAllocator()
    val data = new VarBinaryVector("value", allocator)
    val encoding = new DictionaryEncoding(0L, false, new ArrowType.Int(32, true))
    val keys = new IntVector("value", new FieldType(true, encoding.getIndexType, encoding), allocator)
    val root = VectorSchemaRoot.of(if (dictionary) keys else data)
    val provider = new DictionaryProvider.MapDictionaryProvider(new Dictionary(data, encoding))
    val output = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, if (dictionary) provider else null, output)
    try {
      data.allocateNew()
      keys.allocateNew()
      if (dictionary) {
        values.zipWithIndex.foreach { case (value, index) =>
          if (value == null) data.setNull(index) else data.setSafe(index, value.getBytes(UTF_8))
        }
        data.setValueCount(values.size)
      }
      writer.start()
      batches.foreach { indices =>
        indices.zipWithIndex.foreach { case (index, row) =>
          if (dictionary) {
            if (index < 0) keys.setNull(row) else keys.setSafe(row, index)
          } else {
            if (index < 0 || values(index) == null) data.setNull(row)
            else data.setSafe(row, values(index).getBytes(UTF_8))
          }
        }
        root.setRowCount(indices.size)
        writer.writeBatch()
      }
      writer.end()
    } finally {
      writer.close()
      data.close()
      keys.close()
      allocator.close()
    }
    output.toByteArray
  }
}
