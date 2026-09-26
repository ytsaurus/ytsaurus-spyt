package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, UInt8Vector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.spyt.types.UInt64Type
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** Exercises nested unsigned access and borrowed-buffer ownership without requiring a YT scan. */
class NestedArrowColumnVectorTest extends AnyFlatSpec with Matchers {

  private val unsigned = new ArrowType.Int(64, false)

  private def field(name: String, dataType: ArrowType, children: Field*): Field = {
    new Field(name, FieldType.nullable(dataType), children.asJava)
  }

  private def withRoot(schemaField: Field)(check: VectorSchemaRoot => Unit): Unit = {
    val allocator = new RootAllocator()
    ColumnarResources.using(allocator) {
      val root = VectorSchemaRoot.create(new Schema(Seq(schemaField).asJava), allocator)
      ColumnarResources.using(root) {
        root.allocateNew()
        check(root)
      }
      allocator.getAllocatedMemory shouldBe 0L
    }
  }

  "Nested Arrow views" should "read unsigned struct children and leave the root owned by its caller" in {
    withRoot(field("result", ArrowType.Struct.INSTANCE, field("value", unsigned))) { root =>
      val struct = root.getVector(0).asInstanceOf[StructVector]
      val value = struct.getChild("value").asInstanceOf[UInt8Vector]
      value.setSafe(0, Long.MinValue)
      struct.setIndexDefined(0)
      struct.setNull(1)
      root.setRowCount(2)
      val view = YtColumnarUdfExec.borrowedColumn(struct)
      view.dataType().asInstanceOf[StructType]("value").dataType shouldBe UInt64Type
      view.getStruct(0).getLong(0) shouldBe Long.MinValue
      view.isNullAt(1) shouldBe true
      val batch = new ColumnarBatch(Array(view), root.getRowCount)
      val input = ColumnarArrowInput.create(batch, Seq(0), root.getSchema, struct.getAllocator)
      ColumnarResources.using(input) {
        val nextView = YtColumnarUdfExec.borrowedColumn(input.getVector(0))
        nextView.getStruct(0).getLong(0) shouldBe Long.MinValue
        nextView.isNullAt(1) shouldBe true
      }
      view.close()
      value.get(0) shouldBe Long.MinValue
    }
  }

  it should "read unsigned list elements using the row offsets and preserve element nulls" in {
    withRoot(field("result", ArrowType.List.INSTANCE, field("element", unsigned))) { root =>
      val list = root.getVector(0).asInstanceOf[ListVector]
      val values = list.getDataVector.asInstanceOf[UInt8Vector]
      list.startNewValue(0)
      values.setSafe(0, 1L)
      list.endValue(0, 1)
      list.startNewValue(1)
      values.setSafe(1, Long.MinValue)
      values.setNull(2)
      values.setSafe(3, -1L)
      list.endValue(1, 3)
      root.setRowCount(2)
      val view = YtColumnarUdfExec.borrowedColumn(list)
      view.dataType().asInstanceOf[ArrayType].elementType shouldBe UInt64Type
      val array = view.getArray(1)
      array.numElements() shouldBe 3
      array.getLong(0) shouldBe Long.MinValue
      array.isNullAt(1) shouldBe true
      array.getLong(2) shouldBe -1L
      view.close()
      values.get(3) shouldBe -1L
    }
  }

  it should "read unsigned map keys and ordinary values without releasing their buffers" in {
    val key = new Field("key", FieldType.notNullable(unsigned), Seq.empty[Field].asJava)
    val entries = new Field(
      "entries",
      FieldType.notNullable(ArrowType.Struct.INSTANCE),
      Seq(key, field("value", new ArrowType.Int(32, true))).asJava)
    withRoot(field("result", new ArrowType.Map(false), entries)) { root =>
      val map = root.getVector(0).asInstanceOf[MapVector]
      val pairs = map.getDataVector.asInstanceOf[StructVector]
      val keys = pairs.getChild("key").asInstanceOf[UInt8Vector]
      val values = pairs.getChild("value").asInstanceOf[IntVector]
      map.startNewValue(0)
      keys.setSafe(0, Long.MinValue)
      values.setSafe(0, 7)
      pairs.setIndexDefined(0)
      map.endValue(0, 1)
      root.setRowCount(1)
      val view = YtColumnarUdfExec.borrowedColumn(map)
      view.dataType().asInstanceOf[MapType].keyType shouldBe UInt64Type
      val result = view.getMap(0)
      result.numElements() shouldBe 1
      result.keyArray().getLong(0) shouldBe Long.MinValue
      result.valueArray().getInt(0) shouldBe 7
      view.close()
      keys.get(0) shouldBe Long.MinValue
    }
  }
}
