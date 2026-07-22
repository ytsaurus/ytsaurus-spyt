package tech.ytsaurus.spyt

import org.apache.spark.{Partitioner, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.AdapterSupport420
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanRelation, PushDownUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

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

  override def copyDataSourceV2ScanRelation(rel: DataSourceV2ScanRelation, newScan: Scan): DataSourceV2ScanRelation = {
    rel.copy(scan = newScan)
  }

  override def createCatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String]): CatalogTable = CatalogTable(identifier, tableType, storage, schema, provider=provider)

  override def functionIdentifier(name: String): FunctionIdentifier = AdapterSupport420.functionIdentifier(name)
}
