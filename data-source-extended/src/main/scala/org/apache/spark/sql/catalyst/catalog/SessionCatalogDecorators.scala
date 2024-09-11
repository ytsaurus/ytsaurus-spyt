package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.errors.QueryCompilationErrors
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.net.URI

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.catalog.SessionCatalog")
class SessionCatalogDecorators {

  @DecoratedMethod
  def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit = {
    val isExternal = tableDefinition.tableType == CatalogTableType.EXTERNAL
    if (isExternal && tableDefinition.storage.locationUri.isEmpty) {
      throw QueryCompilationErrors.createExternalTableWithoutLocationError
    }

    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))

    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation = makeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    requireDbExists(db)
    if (tableExists(newTableDefinition.identifier)) {
      if (!ignoreIfExists) {
        if (validateLocation) {
          throw new TableAlreadyExistsException(db = db, table = table)
        } else {
          // Table could be already created by insert operation
          SessionCatalogDecorators.logInsertWarning()
        }
      }
    } else if (validateLocation) {
      validateTableLocation(newTableDefinition)
    }
    externalCatalog.createTable(newTableDefinition, ignoreIfExists)
  }

  // Stubs for methods from base class
  protected[this] def formatDatabaseName(name: String): String = ???
  def getCurrentDatabase: String = ???
  protected[this] def formatTableName(name: String): String = ???
  private def makeQualifiedTablePath(locationUri: URI, database: String): URI = ???
  private def requireDbExists(db: String): Unit = ???
  def tableExists(name: TableIdentifier): Boolean = ???
  def validateTableLocation(table: CatalogTable): Unit = ???
  lazy val externalCatalog: ExternalCatalog  = ???
}

object SessionCatalogDecorators extends Logging {
  def logInsertWarning(): Unit = {
    logWarning("Table existence should not be ignored, but location is already validated. " +
      "So modifiable operation has inserted data already")
  }
}
