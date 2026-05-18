package org.apache.spark.sql

import org.apache.spark.{Partitioner, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.reflect.ClassTag

object AdapterSupport420 {
  def mapPartitionsWithIndexInternal[T, U: ClassTag](
    rdd: RDD[T], f: (Int, Iterator[T]) => Iterator[U], isOrderSensitive: Boolean): RDD[U] = {
    rdd.mapPartitionsWithIndexInternal(f, isOrderSensitive = isOrderSensitive)
  }

  def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    new ShuffleDependency[K, V, C](rdd, partitioner, serializer,
      shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))
  }
}
