package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.util.VectorBatchAppender
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

import tech.ytsaurus.spyt.format.batch.{ArrowColumnVector => YtArrowColumnVector}

import scala.jdk.CollectionConverters._
import scala.collection.mutable

/** Caches independently owned source columns for one batch.
 *
 * Create one instance per source batch and close it after all evaluations. Argument and pass-through roots
 * returned by [[create]] retain their buffers independently and must be closed by their callers.
 */
private[columnar] class ColumnarArrowInput(input: ColumnarBatch, allocator: BufferAllocator) extends AutoCloseable {

  private val cached = mutable.Map.empty[Int, mutable.Buffer[VectorSchemaRoot]]

  /** Reuses compatible converted columns while preserving the requested argument order and schema. */
  def create(ordinals: Seq[Int], schema: Schema): VectorSchemaRoot = {
    require(schema.getFields.size() == ordinals.size, "Columnar input schema does not match the argument count")
    val vectors = ordinals.iterator.zip(schema.getFields.asScala.iterator).map { case (ordinal, field) =>
      val candidates = cached.getOrElseUpdate(ordinal, mutable.ArrayBuffer.empty[VectorSchemaRoot])
      val root = candidates.find(root => ColumnarArrowInput.compatible(root.getVector(0).getField, field))
        .getOrElse {
          val converted = ColumnarArrowInput.create(input, Seq(ordinal), new Schema(Seq(field).asJava), allocator)
          candidates += converted
          converted
        }
      root.getVector(0)
    }
    ColumnarArrowInput.fromArrowVectors(vectors, schema, allocator, input.numRows())
  }

  /** Releases cached columns; previously returned roots retain their own buffer references. */
  override def close(): Unit = {
    try {
      ColumnarResources.closeAll(cached.valuesIterator.flatMap(_.iterator))
    } finally {
      cached.clear()
    }
  }
}

/** Prepares evaluator arguments from Spark batches while retaining compatible Arrow buffers where possible.
 *
 * The execution operator calls [[create]] for each function invocation and closes the returned root after evaluation.
 * The source batch remains owned by the upstream Spark operator.
 */
private[columnar] object ColumnarArrowInput {

  /** Finds a directly usable Arrow vector; dictionary-encoded YT columns require the conversion path. */
  private def rawVector(column: ColumnVector): Option[FieldVector] = column match {
    case arrow: NestedArrowColumnVector => Some(arrow.getValueVector)
    case arrow: YtArrowColumnVector if !arrow.isDictionaryEncoded =>
      Option(arrow.getValueVector).collect { case vector: FieldVector => vector }
    case arrow: ArrowColumnVector =>
      Option(arrow.getValueVector).collect { case vector: FieldVector => vector }
    case _ => None
  }

  /** Allows top-level renaming and nullability changes, but requires nested names for Arrow's named transfers. */
  private def compatible(actual: Field, expected: Field): Boolean = {
    actual.getDictionary == null && actual.getType == expected.getType &&
      actual.getChildren.size() == expected.getChildren.size() &&
      actual.getChildren.asScala.iterator.zip(expected.getChildren.asScala.iterator).forall { case (a, e) =>
        a.getName == e.getName && compatible(a, e)
      }
  }

  /** Builds an owned Arrow root with arguments in the order required by the provider's input schema.
   *
   * Compatible Arrow columns share buffers only within the supplied allocator's root. Columns from other roots
   * are copied so closing an upstream allocator cannot invalidate retained results. If any argument is
   * incompatible, all arguments are written through Spark's Arrow writer.
   * The caller must close the root after evaluation, including on failure.
   *
   * @param input borrowed source batch, which is neither closed nor modified
   * @param ordinals source column indices, one per declared input field
   * @param schema provider input schema used for the returned root
   * @param allocator allocator owning the returned vectors and any copied buffers
   * @return owned argument root whose lifetime is independent of the source vector wrappers
   */
  def create(input: ColumnarBatch, ordinals: Seq[Int], schema: Schema, allocator: BufferAllocator): VectorSchemaRoot = {
    val fields = schema.getFields.asScala
    require(fields.size == ordinals.size, "Columnar input schema does not match the argument count")
    val sources = ordinals.map(input.column)
    val raw = sources.iterator.zip(fields.iterator).map { case (column, field) =>
      rawVector(column).filter(vector => compatible(vector.getField, field))
    }.toVector
    if (raw.forall(_.isDefined)) {
      fromArrowVectors(raw.iterator.map(_.get), schema, allocator, input.numRows())
    } else {
      fromRows(sources, schema, allocator, input.numRows())
    }
  }

  /** Transfers compatible vectors, copying across allocator roots to keep the result independently owned. */
  private def fromArrowVectors(
    sources: Iterator[FieldVector],
    schema: Schema,
    allocator: BufferAllocator,
    rowCount: Int): VectorSchemaRoot = {
    val vectors = mutable.ArrayBuffer.empty[FieldVector]
    ColumnarResources.withCleanupOnFailure(ColumnarResources.closeAll(vectors.iterator)) {
      sources.zip(schema.getFields.asScala.iterator).foreach { case (source, field) =>
        val target = field.createVector(allocator)
        vectors += target
        if (source.getAllocator.getRoot == allocator.getRoot) {
          val transfer = source.makeTransferPair(target)
          transfer.splitAndTransfer(0, rowCount)
        } else if (source.getValueCount == rowCount &&
          (source.isInstanceOf[BaseFixedWidthVector] || source.isInstanceOf[BaseVariableWidthVector])) {
          if (rowCount > 0) {
            target.setInitialCapacity(rowCount)
            target.allocateNew()
            VectorBatchAppender.batchAppend(target, source)
          }
        } else {
          (0 until rowCount).foreach(row => target.copyFromSafe(row, row, source))
          target.setValueCount(rowCount)
        }
      }
      new VectorSchemaRoot(schema, vectors.asJava, rowCount)
    }
  }

  /** Converts selected columns through Spark's row writer when their Arrow vectors cannot be transferred. */
  private def fromRows(
    sources: Seq[ColumnVector],
    schema: Schema,
    allocator: BufferAllocator,
    rowCount: Int): VectorSchemaRoot = {
    val converted = VectorSchemaRoot.create(schema, allocator)
    ColumnarResources.withCleanupOnFailure(converted.close()) {
      val writer = ArrowWriter.create(converted)
      val rows = new ColumnarBatch(sources.toArray, rowCount).rowIterator()
      while (rows.hasNext) {
        writer.write(rows.next())
      }
      writer.finish()
      converted
    }
  }
}
