package org.apache.spark.sql.v2
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources.Filter

class YtScanBuilder330(scanBuilderAdapter: ScanBuilderAdapter) extends YtScanBuilderBase(scanBuilderAdapter) {

  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    val dataFilters = super.pushFilters(filters)
    scanBuilderAdapter.setPartitionFilters(this.partitionFilters)
    scanBuilderAdapter.setDataFilters(this.dataFilters)
    dataFilters
  }

  override def pushDataFilters(filters: Array[Filter]): Array[Filter] = scanBuilderAdapter.pushFilters(filters)

  override def pushedFilters: Array[Predicate] = scanBuilderAdapter.pushedFilters().map(_.toV2)
}
