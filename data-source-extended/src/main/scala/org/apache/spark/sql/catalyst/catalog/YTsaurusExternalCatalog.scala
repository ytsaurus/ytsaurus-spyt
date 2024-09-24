package org.apache.spark.sql.catalyst.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.serializers.SchemaConverter.{Sorted, Unordered}
import tech.ytsaurus.spyt.serializers.{SchemaConverter, SchemaConverterConfig, WriteSchemaConverter}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.table.BaseYtTableSettings
import tech.ytsaurus.ysontree.{YTreeNode, YTreeTextSerializer}

import java.net.URI
import java.util.UUID
import java.util.stream.Collectors

class YTsaurusExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends InMemoryCatalog(conf, hadoopConf) with Logging {

  private val idPrefix: String = s"YTsaurusExternalCatalog-${UUID.randomUUID()}"
  private val ytConf = ytClientConfiguration(hadoopConf)

  private val YTSAURUS_DB = "yt"

  private lazy val ytDatabase = CatalogDatabase(YTSAURUS_DB, "", new URI(""), Map.empty)

  private def isYtDatabase(db: String): Boolean = db == YTSAURUS_DB
  private def isYtDatabase(db: Option[String]): Boolean = db.exists(isYtDatabase)

  private def checkUnsupportedDatabase(db: String): Unit = {
    if (isYtDatabase(db)) {
      throw new IllegalStateException("Operation is not supported for YTsaurus tables")
    }
  }

  private def checkUnsupportedDatabase(db: Option[String]): Unit = db.foreach(checkUnsupportedDatabase)

  override def databaseExists(db: String): Boolean = isYtDatabase(db) || super.databaseExists(db)

  override def getDatabase(db: String): CatalogDatabase = {
    if (isYtDatabase(db)) {
      ytDatabase
    } else {
      super.getDatabase(db)
    }
  }

  private val excludeKeys = Set("key_columns", "unique_keys")

  private def getSortOption(extraProperties: Map[String, YTreeNode]): SchemaConverter.SortOption = {
    import scala.collection.JavaConverters._
    val keyColumns = extraProperties.get("key_columns").map(_.asList().asScala.map(_.stringValue())).getOrElse(Seq())
    if (keyColumns.nonEmpty) {
      Sorted(keyColumns, extraProperties.get("unique_keys").exists(_.boolValue()))
    } else {
      Unordered
    }
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = synchronized {
    if (isYtDatabase(tableDefinition.identifier.database)) {
      val path = YPathEnriched.fromString(tableDefinition.identifier.table)
      val yt = YtClientProvider.ytClientWithProxy(ytConf, path.cluster, idPrefix)
      if (!YtWrapper.exists(path.toStringYPath)(yt)) {
        val extraProperties = tableDefinition.properties.mapValues(YTreeTextSerializer.deserialize)
        val tableSchema = new WriteSchemaConverter().tableSchema(tableDefinition.schema, getSortOption(extraProperties))
        val settings = new BaseYtTableSettings(tableSchema, extraProperties.filterKeys(!excludeKeys.contains(_)))
        YtWrapper.createTable(path.toStringYPath, settings)(yt)
      } else {
        logWarning("Table is created twice. Ignoring new query")
      }
    } else {
      super.createTable(tableDefinition, ignoreIfExists)
    }
  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = synchronized {
    if (isYtDatabase(db)) {
      val path = YPathEnriched.fromString(table)
      val yt = YtClientProvider.ytClientWithProxy(ytConf, path.cluster, idPrefix)
      if (ignoreIfNotExists) {
        YtWrapper.removeIfExists(path.toStringYPath)(yt)
      } else {
        YtWrapper.remove(path.toStringYPath)(yt)
      }
    } else {
      super.dropTable(db, table, ignoreIfNotExists, purge)
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    checkUnsupportedDatabase(db)
    super.renameTable(db, oldName, newName)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    checkUnsupportedDatabase(tableDefinition.identifier.database)
    super.alterTable(tableDefinition)
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    checkUnsupportedDatabase(db)
    super.alterTableDataSchema(db, table, newDataSchema)
  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = {
    checkUnsupportedDatabase(db)
    super.alterTableStats(db, table, stats)
  }

  private def getYtTableOptional(table: String): Option[CatalogTable] = {
    val path = YPathEnriched.fromString(table)
    val yt = YtClientProvider.ytClientWithProxy(ytConf, path.cluster, idPrefix)
    if (YtWrapper.exists(path.toStringYPath)(yt)) {
      val ident = TableIdentifier(table, Some(YTSAURUS_DB))
      val config = SchemaConverterConfig(conf)
      val schemaTree = YtWrapper.attribute(path.toStringYPath, "schema")(yt)
      val schema = SchemaConverter.sparkSchema(schemaTree, parsingTypeV3 = config.parsingTypeV3)
      val storage = CatalogStorageFormat(
        locationUri = Some(path.toPath.toUri),
        inputFormat = None, outputFormat = None, serde = None, compressed = false, properties = Map.empty
      )
      Some(CatalogTable(ident, CatalogTableType.MANAGED, storage, schema, provider = Some("yt")))
    } else {
      None
    }
  }

  override def getTable(db: String, table: String): CatalogTable = synchronized {
    if (isYtDatabase(db)) {
      getYtTableOptional(table).getOrElse {
        throw new NoSuchTableException(db = db, table = table)
      }
    } else {
      super.getTable(db, table)
    }
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = synchronized {
    if (isYtDatabase(db)) {
      tables.flatMap(getYtTableOptional)
    } else {
      super.getTablesByName(db, tables)
    }
  }

  override def tableExists(db: String, table: String): Boolean = {
    if (isYtDatabase(db)) {
      val path = YPathEnriched.fromString(table)
      val yt = YtClientProvider.ytClientWithProxy(ytConf, path.cluster, idPrefix)
      YtWrapper.exists(path.toYPath, None)(yt)
    } else {
      super.tableExists(db, table)
    }
  }

  override def listTables(db: String): Seq[String] = {
    checkUnsupportedDatabase(db)
    super.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    checkUnsupportedDatabase(db)
    super.listTables(db, pattern)
  }

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit = {
    checkUnsupportedDatabase(db)
    super.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }
}
