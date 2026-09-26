package tech.ytsaurus.spyt.format.columnar

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/** Owns the Arrow roots backing a plugin projection's borrowed column views.
 *
 * Construct after evaluating all columns successfully. Closing the batch releases roots exactly once;
 * the column views themselves do not own their vectors. The supplied roots must not be modified after construction.
 */
private[columnar] class ColumnarFunctionBatch(
  columns: Array[ColumnVector],
  rowCount: Int,
  roots: scala.collection.Seq[VectorSchemaRoot]) extends ColumnarBatch(columns, rowCount) {

  private var released = false

  /** Releases owned pass-through views and plugin output roots once. */
  override def close(): Unit = if (!released) {
    released = true
    ColumnarResources.closeAll(roots.iterator)
  }
}
