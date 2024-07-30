package org.apache.spark.sql.v2

import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.YtFileFormat
import tech.ytsaurus.spyt.format.GlobalTransactionUtils
import tech.ytsaurus.spyt.fs.path.YPathEnriched

class YtDataSourceV2 extends FileDataSourceV2 with SessionConfigSupport {
  private val defaultOptions: Map[String, String] = Map()

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[YtFileFormat]

  override def shortName(): String = "yt"

  override protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings._
    import tech.ytsaurus.spyt.fs.conf._

    val paths = super.getPaths(options)
    val transaction = options.getYtConf(Transaction).orElse(GlobalTransactionUtils.getGlobalTransactionId(sparkSession))
    val timestamp = options.getYtConf(Timestamp)
    val inconsistentReadEnabled = options.ytConf(InconsistentReadEnabled)

    if (inconsistentReadEnabled && timestamp.nonEmpty) {
      throw new IllegalStateException("Using of both timestamp and enable_inconsistent_read options is prohibited")
    }

    paths.map { s =>
      val path = YPathEnriched.fromString(s)
      val transactionYPath = transaction.map(path.withTransaction).getOrElse(path)
      val versionedPath = if (inconsistentReadEnabled) {
        transactionYPath.withLatestVersion
      } else {
        timestamp.map(transactionYPath.withTimestamp).getOrElse(transactionYPath)
      }
      versionedPath.toStringPath
    }
  }

  private def getOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    import scala.collection.JavaConverters._
    val opts = defaultOptions ++ options.asScala
    new CaseInsensitiveStringMap(opts.asJava)
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    YtTable(tableName, sparkSession, getOptions(options), paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    YtTable(tableName, sparkSession, getOptions(options), paths, Some(schema), fallbackFileFormat)
  }

  override def keyPrefix(): String = "yt"
}
