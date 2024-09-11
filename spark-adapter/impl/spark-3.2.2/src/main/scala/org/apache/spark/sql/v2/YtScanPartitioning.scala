package org.apache.spark.sql.v2

import org.apache.spark.sql.connector.read.partitioning.{Distribution, Partitioning}

case class YtScanPartitioning (partitionsCount: Int) extends Partitioning with Serializable {
  override def numPartitions(): Int = partitionsCount

  override def satisfy(distribution: Distribution): Boolean = false
}
