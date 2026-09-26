package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._

/** Verifies independent buffer ownership and reuse when preparing Arrow arguments across allocator roots. */
class ColumnarArrowInputTest extends AnyFlatSpec with Matchers {

  /** Closes the resource after the check, preserving any primary failure. */
  private def withResource[T <: AutoCloseable, R](resource: T)(check: T => R): R = {
    ColumnarResources.using(resource)(check(resource))
  }

  private val fields = Seq(
    new Field("number", FieldType.nullable(new ArrowType.Int(64, true)), null),
    new Field("text", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
    new Field("flag", FieldType.nullable(ArrowType.Bool.INSTANCE), null))
  private val schema = new Schema(fields.asJava)

  for (rowCount <- Seq(0, 1, 9, 10000); extraValues <- Seq(0, 3)) {
    "Arrow input" should s"preserve $rowCount rows with $extraValues trailing source values across roots" in {
      withResource(new RootAllocator()) { destination =>
        val copied = withResource(new RootAllocator()) { upstream =>
          withResource(VectorSchemaRoot.create(schema, upstream)) { source =>
            source.allocateNew()
            val numbers = source.getVector(0).asInstanceOf[BigIntVector]
            val strings = source.getVector(1).asInstanceOf[VarCharVector]
            val flags = source.getVector(2).asInstanceOf[BitVector]
            for (row <- 0 until rowCount + extraValues) {
              if (row % 3 != 1) {
                numbers.setSafe(row, row.toLong)
                strings.setSafe(row, s"value-$row".getBytes(UTF_8))
                flags.setSafe(row, row % 2)
              }
            }
            source.setRowCount(rowCount + extraValues)
            val batch = new ColumnarBatch(
              source.getFieldVectors.asScala.map(YtColumnarUdfExec.borrowedColumn).toArray, rowCount)
            ColumnarArrowInput.create(batch, Seq(0, 1, 2), schema, destination)
          }
        }
        withResource(copied) { root =>
          root.getRowCount shouldBe rowCount
          root.getFieldVectors.asScala.foreach(_.getValueCount shouldBe rowCount)
          for (row <- 0 until rowCount) {
            if (row % 3 == 1) {
              root.getFieldVectors.asScala.foreach(_.isNull(row) shouldBe true)
            } else {
              root.getVector(0).asInstanceOf[BigIntVector].get(row) shouldBe row.toLong
              root.getVector(1).getObject(row).toString shouldBe s"value-$row"
              root.getVector(2).asInstanceOf[BitVector].get(row) shouldBe row % 2
            }
          }
        }
        destination.getAllocatedMemory shouldBe 0L
      }
    }
  }

  it should "reuse copied columns across renamed arguments and pass-through roots" in {
    withResource(new RootAllocator()) { destination =>
      val (passThrough, arguments) = withResource(new RootAllocator()) { upstream =>
        withResource(VectorSchemaRoot.create(new Schema(Seq(fields.head).asJava), upstream)) { source =>
          source.allocateNew()
          source.getVector(0).asInstanceOf[BigIntVector].setSafe(0, 42L)
          source.setRowCount(1)
          val batch = new ColumnarBatch(Array(YtColumnarUdfExec.borrowedColumn(source.getVector(0))), 1)
          withResource(new ColumnarArrowInput(batch, destination)) { input =>
            val passThrough = input.create(Seq(0), source.getSchema)
            val allocated = destination.getAllocatedMemory
            val renamed = new Field("argument", FieldType.notNullable(new ArrowType.Int(64, true)), null)
            val arguments = input.create(Seq(0, 0), new Schema(Seq(renamed, fields.head).asJava))
            destination.getAllocatedMemory shouldBe allocated
            arguments.getSchema.getFields.get(0) shouldBe renamed
            arguments.getFieldVectors.asScala.foreach { vector =>
              vector.getDataBuffer.memoryAddress() shouldBe passThrough.getVector(0).getDataBuffer.memoryAddress()
            }
            (passThrough, arguments)
          }
        }
      }
      passThrough.close()
      withResource(arguments) { root =>
        root.getFieldVectors.asScala.foreach(_.asInstanceOf[BigIntVector].get(0) shouldBe 42L)
      }
      destination.getAllocatedMemory shouldBe 0L
    }
  }
}
