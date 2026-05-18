package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.vectorized.ColumnarBatch

trait FileSourceScanExecDelegate { self: FileSourceScanExec =>
  def doExecuteInternal(): RDD[InternalRow] = this.doExecute()
  def doExecuteColumnarInternal(): RDD[ColumnarBatch] = this.doExecuteColumnar()
  @transient
  private lazy val pushedDownFiltersMethod = {
    val m = self.getClass.getSuperclass.getDeclaredMethod("pushedDownFilters")
    m.setAccessible(true)
    m
  }

  def pushedDownFiltersInternal(): Seq[Filter] =
    scala.util.Try {
      pushedDownFiltersMethod.invoke(this).asInstanceOf[Seq[Filter]]
    }.getOrElse(Seq.empty)

}
