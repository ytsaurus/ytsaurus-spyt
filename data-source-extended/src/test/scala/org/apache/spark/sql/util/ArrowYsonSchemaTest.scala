package org.apache.spark.sql.util

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.spyt.types.YsonType
import org.apache.spark.sql.types.{BinaryType, CalendarIntervalType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.test.LocalSpark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets

class ArrowYsonSchemaTest extends AnyFlatSpec with Matchers with LocalSpark {

  behavior of "Arrow YSON schema"

  Seq(false, true).foreach { losslessInternalTypes =>
    it should s"preserve losslessInternalTypes=$losslessInternalTypes with YSON on Spark 4.3" in {
      whenSparkVersionAtLeast("4.3.0") {
        spark
        val schema = StructType(Seq(
          StructField("payload", new YsonType()),
          StructField("interval", CalendarIntervalType)))
        // The five-argument overload is absent from the Spark 3.5 compile classpath.
        val method = ArrowUtils.getClass.getMethod(
          "toArrowSchema",
          classOf[StructType],
          classOf[String],
          java.lang.Boolean.TYPE,
          java.lang.Boolean.TYPE,
          java.lang.Boolean.TYPE)
        val arrowSchema = method.invoke(
          ArrowUtils,
          schema,
          "UTC",
          Boolean.box(true),
          Boolean.box(false),
          Boolean.box(losslessInternalTypes)).asInstanceOf[Schema]

        arrowSchema.findField("payload").getMetadata.get("YtType") shouldBe "yson"
        val expectedType = if (losslessInternalTypes) ArrowType.Struct.INSTANCE else {
          new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
        }
        arrowSchema.findField("interval").getType shouldBe expectedType
      }
    }
  }

  for {
    (dataType, expectedYtType) <- Seq((new YsonType(), Some("yson")), (BinaryType, None))
    largeVarTypes <- Seq(false, true)
    nullable <- Seq(false, true)
  } {
    it should s"preserve ${dataType.catalogString} in Arrow with largeVarTypes=$largeVarTypes, nullable=$nullable" in {
      spark
      val schema = StructType(Seq(
        StructField("payload", dataType, nullable),
        StructField("text", StringType)))
      val arrowSchema = ArrowUtils.toArrowSchema(schema, "UTC", true, largeVarTypes)
      val expectedType = if (largeVarTypes) ArrowType.LargeBinary.INSTANCE else ArrowType.Binary.INSTANCE
      val payload = arrowSchema.findField("payload")
      Option(payload.getMetadata.get("YtType")) shouldBe expectedYtType
      payload.getType shouldBe expectedType
      payload.isNullable shouldBe nullable
      arrowSchema.findField("text").getMetadata.containsKey("YtType") shouldBe false

      val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
      val output = new ByteArrayOutputStream()
      val writer = new ArrowStreamWriter(root, null, output)
      val ysonBytes = "{value=42;items=[1u;18446744073709551615u;];}".getBytes(StandardCharsets.UTF_8)
      try {
        val rowWriter = ArrowWriter.create(root)
        rowWriter.write(new GenericInternalRow(Array[Any](ysonBytes, UTF8String.fromString("text"))))
        rowWriter.finish()
        writer.start()
        writer.writeBatch()
        writer.end()
      } finally {
        writer.close()
        root.close()
      }
      val reader = new ArrowStreamReader(new ByteArrayInputStream(output.toByteArray), ArrowUtils.rootAllocator)
      try {
        reader.loadNextBatch() shouldBe true
        val restoredRoot = reader.getVectorSchemaRoot
        restoredRoot.getSchema shouldBe arrowSchema
        restoredRoot.getRowCount shouldBe 1
        restoredRoot.getVector("payload").getObject(0).asInstanceOf[Array[Byte]] shouldBe ysonBytes
      } finally {
        reader.close()
      }
    }
  }
}
