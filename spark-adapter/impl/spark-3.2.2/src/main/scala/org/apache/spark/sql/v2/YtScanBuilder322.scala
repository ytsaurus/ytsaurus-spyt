package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class YtScanBuilder322(scanBuilderAdapter: ScanBuilderAdapter)
  extends YtScanBuilderBase(scanBuilderAdapter) with SupportsPushDownFilters {

  override def pushFilters(filters: Array[Filter]): Array[Filter] = scanBuilderAdapter.pushFilters(filters)

  override def pushedFilters(): Array[Filter] = scanBuilderAdapter.pushedFilters()
}
