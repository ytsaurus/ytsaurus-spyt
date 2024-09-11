package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait ScanBuilderAdapter {
  def sparkSession: SparkSession
  def fileIndex: PartitioningAwareFileIndex
  def schema: StructType
  def dataSchema: StructType
  def options: CaseInsensitiveStringMap

  def setPartitionFilters(partitionFilters: Seq[Expression]): Unit
  def setDataFilters(dataFilters: Seq[Expression]): Unit
  def pushFilters(filters: Array[Filter]): Array[Filter]
  def pushedFilters(): Array[Filter]
  def build(dataSchema: StructType, partitionSchema: StructType): Scan
}
