package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import tech.ytsaurus.spyt.common.utils.TuplePoint

//Actually it is range partitioning pretending to be hash partitioning
class DependentHashPartitioning(expressions: Seq[Expression], val pivots: Seq[TuplePoint])
  extends HashPartitioning(expressions, numPartitions = pivots.length + 1) {

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): DependentHashPartitioning = {
    new DependentHashPartitioning(newChildren, pivots)
  }
}
