package org.apache.spark.sql.v2

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, Table}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.YtFileFormat
import tech.ytsaurus.spyt.format.GlobalTransactionUtils
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.WriteTransaction
import tech.ytsaurus.spyt.fs.path.YPathEnriched

import scala.jdk.CollectionConverters._

class YtDataSourceV2 extends FileDataSourceV2 with SessionConfigSupport {
  private val defaultOptions: Map[String, String] = Map()

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[YtFileFormat]

  override def shortName(): String = "yt"

  override protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings._
    import tech.ytsaurus.spyt.wrapper.config._

    val paths = super.getPaths(options)
    val transaction = options.getYtConf(Transaction)
      .orElse(GlobalTransactionUtils.getGlobalTransactionId(sparkSession)).filter(_.nonEmpty)
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

  def sessionOptions: Map[String, String] = {
    DataSourceV2Utils.extractSessionConfigs(this, sparkSession.sessionState.conf)
  }
}

object YtDataSourceV2 {

  def validateWriteTransactionDefault(sqlConf: SQLConf, options: Map[String, String]): Unit = {
    if (!CaseInsensitiveMap(options).contains(WriteTransaction.name)) {
      writeTransactionDefault(sqlConf).filter(_.nonEmpty).foreach { transaction =>
        throw new IllegalStateException(
          s"Session write transaction $transaction was not applied during output planning. " +
            "Set write_transaction to an empty string to opt out.")
      }
    }
  }

  def withWriteTransactionDefault(sqlConf: SQLConf, options: Map[String, String]): Map[String, String] = {
    val defaults = writeTransactionDefault(sqlConf).map(WriteTransaction.name -> _).toMap
    CaseInsensitiveMap(defaults) ++ options
  }

  private def writeTransactionDefault(sqlConf: SQLConf): Option[String] = {
    Option(sqlConf.getConfString(s"spark.datasource.yt.${WriteTransaction.name}", null))
  }
}
