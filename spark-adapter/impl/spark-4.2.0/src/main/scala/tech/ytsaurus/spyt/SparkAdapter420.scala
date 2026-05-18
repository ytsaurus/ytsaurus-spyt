package tech.ytsaurus.spyt

import org.apache.spark.{Partitioner, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.AdapterSupport420
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, PushDownUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sources.Filter

import java.net.URI
import scala.reflect.ClassTag

trait SparkAdapter420 extends SparkAdapter {
  override def pushFilters(
    scanBuilder: ScanBuilder, filters: Seq[Expression]): Either[Seq[Filter], Seq[Predicate]] = {
    PushDownUtils.pushFilters(scanBuilder, filters, None)._1
  }

  override def getInputPartition(dsrddPartition: DataSourceRDDPartition): InputPartition = {
    dsrddPartition.inputPartition.get
  }

  override def createCatalogStorageFormat(locationUri: Option[URI]): CatalogStorageFormat = {
    CatalogStorageFormat(
      locationUri = locationUri, inputFormat = None, outputFormat = None, serde = None,
      compressed = false, properties = Map.empty, serdeName = None
    )
  }

  override def mapPartitionsWithIndexInternal[T, U: ClassTag](
    rdd: RDD[T], f: (Int, Iterator[T]) => Iterator[U], isOrderSensitive: Boolean): RDD[U] = {
    AdapterSupport420.mapPartitionsWithIndexInternal(rdd, f, isOrderSensitive)
  }

  override def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    AdapterSupport420.createShuffleDependency(rdd, partitioner, serializer, writeMetrics)
  }
}
