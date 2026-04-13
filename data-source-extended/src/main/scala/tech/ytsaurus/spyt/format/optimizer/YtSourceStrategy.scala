package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.FileSourceStrategy
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.yt.YtSourceScanExec

/**
 * A strategy for planning scans over YTsaurus tables that delegates to Spark's built-in
 * [[FileSourceStrategy]] and replaces [[FileSourceScanExec]] nodes with [[YtSourceScanExec]].
 *
 * The only YT-specific behavior is the replacement of the scan node, which enables YT-specific
 * optimizations (scan mode detection, read parallelism, columnar batch support) inside
 * [[YtSourceScanExec]].
 */
class YtSourceStrategy extends Strategy {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val basePlans = FileSourceStrategy.apply(plan)
    if (basePlans.isEmpty) {
      Nil
    } else {
      basePlans.map(replaceFileSourceScan)
    }
  }

  private def replaceFileSourceScan(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec =>
      logDebug(s"Replacing FileSourceScanExec with YtSourceScanExec for ${scan.tableIdentifier}")
      YtSourceScanExec(
        relation = scan.relation,
        output = scan.output,
        requiredSchema = scan.requiredSchema,
        partitionFilters = scan.partitionFilters,
        optionalBucketSet = scan.optionalBucketSet,
        dataFilters = scan.dataFilters,
        tableIdentifier = scan.tableIdentifier
      )
    case other =>
      other.withNewChildren(other.children.map(replaceFileSourceScan))
  }
}
