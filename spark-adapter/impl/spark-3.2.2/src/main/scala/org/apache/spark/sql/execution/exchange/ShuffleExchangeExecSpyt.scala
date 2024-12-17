package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import tech.ytsaurus.spyt.patch.annotations.{Applicability, OriginClass, Subclass}

/**
 * This is a backward compatibility class that allows to use SPYT with older Spark versions, which has
 * 3-arg primary constructor. Also the withNewChildInternal is overridden because it uses copy method.
 */
@Subclass
@OriginClass("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec")
@Applicability(to = "3.4.4")
class ShuffleExchangeExecSpyt(outputPartitioning: Partitioning,
                              child: SparkPlan,
                              shuffleOrigin: ShuffleOrigin,
                              advisoryPartitionSizeCompat: Option[Long]
                             ) extends ShuffleExchangeExec(outputPartitioning, child, shuffleOrigin) {

  def this(outputPartitioning: Partitioning, child: SparkPlan, shuffleOrigin: ShuffleOrigin) = {
    this(outputPartitioning, child, shuffleOrigin, None)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ShuffleExchangeExecSpyt = {
    new ShuffleExchangeExecSpyt(outputPartitioning, newChild, shuffleOrigin, advisoryPartitionSizeCompat)
  }

}
