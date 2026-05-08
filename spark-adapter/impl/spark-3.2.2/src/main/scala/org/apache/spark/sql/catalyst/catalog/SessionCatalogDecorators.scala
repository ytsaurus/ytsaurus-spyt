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
    try {
      __createTable(tableDefinition, ignoreIfExists, validateLocation)
    } catch {
      case taee: TableAlreadyExistsException => if (validateLocation) {
        throw taee
      } else {
        // Table could be already created by insert operation
        SessionCatalogDecorators.logInsertWarning()
      }
    }
  }

  def __createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean, validateLocation: Boolean): Unit = ???

  @DecoratedMethod()
  private def validateName(name: String): Unit = {
    // Replacing original method with NOP implementation because YTsaurus table names doesn't conform to
    // original validNameFormat regex
  }

  private def __validateName(name: String): Unit = ???
}

object SessionCatalogDecorators extends Logging {
  def logInsertWarning(): Unit = {
    logWarning("Table existence should not be ignored, but location is already validated. " +
      "So modifiable operation has inserted data already")
  }
}
