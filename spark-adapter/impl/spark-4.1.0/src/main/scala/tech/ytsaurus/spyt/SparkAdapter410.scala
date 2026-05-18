package tech.ytsaurus.spyt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.{Partitioner, ShuffleDependency, SparkConf}
import org.apache.spark.sql.{AdapterSupport410, AnalysisException}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.runtime.SerializedOffset
import org.apache.spark.sql.execution.streaming.sinks.FileStreamSink
import org.apache.spark.sql.vectorized.ColumnarBatchRowWrapperBase
import org.apache.spark.unsafe.types.{GeographyVal, GeometryVal, VariantVal}

import java.io.File
import java.util.Locale
import scala.reflect.ClassTag

trait SparkAdapter410 extends SparkAdapter {

  override def createAnalysisException(message: String): AnalysisException = {
    new AnalysisException(message, None, None, None, None, Map.empty, Array.empty)
  }

  override def filterRootPaths(rootPathsSpecified: Seq[Path], hadoopConf: Configuration): Seq[Path] = {
    rootPathsSpecified.filterNot(FileStreamSink.ancestorIsMetadataDirectory(_, hadoopConf))
  }

  override def copyDataSourceV2Relation(rel: DataSourceV2Relation, table: Table): DataSourceV2Relation = {
    rel.copy(table = table)
  }

  override def isUint64DataTypeContext(ctx: SqlBaseParser.PrimitiveDataTypeContext, identifierId: Int): Boolean = {
    ctx.primitiveType().start.getType == identifierId &&
      ctx.primitiveType().start.getText.toLowerCase(Locale.ROOT) == "uint64"
  }

  override def createShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    rdd: RDD[_ <: Product2[K, V]],
    partitioner: Partitioner,
    serializer: Serializer,
    writeMetrics: Map[String, SQLMetric]): ShuffleDependency[K, V, C] = {
    AdapterSupport410.createShuffleDependency(rdd, partitioner, serializer, writeMetrics)
  }

  override def offsetAsJson(offset: Offset): String = offset match {
    case sv: SerializedOffset => sv.json
    case _ => throw new IllegalArgumentException("Unsupported offset format")
  }

  override def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean)): Seq[String] = {
    AdapterSupport410.sparkJavaOpts(conf, filterKey)
  }

  override def createColumnarBatchRowWrapper(
    row: InternalRow,
    getStructImpl: (Int, Int) => InternalRow,
    getArrayImpl: Int => ArrayData,
    getMapImpl: Int => MapData): InternalRow = {
    new ColumnarBatchRowWrapperBase(row, getStructImpl, getArrayImpl, getMapImpl) {
      override def getVariant(ordinal: Int): VariantVal = row.getVariant(ordinal)
      override def getGeography(ordinal: Int): GeographyVal = row.getGeography(ordinal)
      override def getGeometry(ordinal: Int): GeometryVal = row.getGeometry(ordinal)
    }
  }

  override def getHadoopConf(sparkConf: SparkConf): Configuration = AdapterSupport410.getHadoopConf(sparkConf)

  override def fetchFile(url: String, targetDir: File, conf: SparkConf): File = {
    AdapterSupport410.fetchFile(url, targetDir, conf)
  }
}
