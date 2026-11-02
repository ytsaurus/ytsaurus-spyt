package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import tech.ytsaurus.spyt.patch.annotations.{AddMethod, Applicability, Decorate, DecoratedMethod, OriginClass, Subclass}

@Subclass
@OriginClass("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec")
@Applicability(from = "4.0.0", to = "4.3.0")
class ShuffleExchangeExecSpyt400(
    outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin,
    advisoryPartitionSize: Option[Long],
    pipelinedCompat: Boolean)
  extends ShuffleExchangeExec(outputPartitioning, child, shuffleOrigin, advisoryPartitionSize) {

  def this(
    outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin,
    advisoryPartitionSize: Option[Long]) = {
    this(outputPartitioning, child, shuffleOrigin, advisoryPartitionSize, false)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ShuffleExchangeExecSpyt400 = {
    new ShuffleExchangeExecSpyt400(outputPartitioning, newChild, shuffleOrigin, advisoryPartitionSize, pipelinedCompat)
  }

}

@Decorate
@OriginClass("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$")
@Applicability(from = "4.0.0", to = "4.3.0")
object ShuffleExchangeExecSpyt400 {

  @AddMethod
  def $lessinit$greater$default$5(): Boolean = false
}
