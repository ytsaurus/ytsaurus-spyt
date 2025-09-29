package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.StreamingUtils.{ROW_INDEX_WITH_PREFIX, STREAMING_SERVICE_KEY_COLUMNS_PREFIX, TABLET_INDEX_WITH_PREFIX}
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.client.rows.{QueueRowset, UnversionedRowset, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnSchema, TableSchema}

class UnversionedRowsetDeserializer(schema: StructType) {
  private var deserializer: InternalRowDeserializer = _

  private def deserializeValue(value: UnversionedValue): Unit = {
    value.getValue.asInstanceOf[Any] match {
      case v if v == null => deserializer.onEntity()
      case v: Boolean => deserializer.onBoolean(v)
      case v: Long => deserializer.onInteger(v)
      case v: Double => deserializer.onDouble(v)
      case v: Array[Byte] => deserializer.onBytes(v)
      case v => throw new MatchError(v)
    }
  }

  private def deserializeValues(values: Seq[UnversionedValue]): InternalRow = {
    deserializer.onNewRow(schema.length)
    values.foreach { value =>
      deserializer.setId(value.getId)
      deserializer.setType(value.getType)
      deserializeValue(value)
    }
    deserializer.onCompleteRow()
  }

  def deserializeRowset(rowset: UnversionedRowset): Iterator[InternalRow] = {
    import scala.collection.JavaConverters._
    deserializer = InternalRowDeserializer.getOrCreate(schema)
    deserializer.updateSchema(rowset.getSchema)
    rowset.getRows.asScala.iterator.map { row =>
      val rowValues = row.getValues.asScala
      val values = schema.fields.map { field =>
        rowValues.find(value => rowset.getSchema.getColumnName(value.getId) == field.name)
          .getOrElse(throw new IllegalStateException(s"${field.name} is not found in rowset"))
      }
      deserializeValues(values)
    }
  }

  def deserializeQueueRowsetWithServiceColumns(rowset: QueueRowset): Iterator[InternalRow] = {
    import scala.collection.JavaConverters._
    deserializer = InternalRowDeserializer.getOrCreate(schema)
    val changedYtSchema = getSchemaColumnsWithChangedServiceColumnsNames(rowset.getSchema)
    deserializer.updateSchema(changedYtSchema)
    rowset.getRows.asScala.iterator.map { row =>
      val uvMap = row.getValues.asScala.map { value =>
        changedYtSchema.getColumnName(value.getId) -> value
      }.toMap

      val keys = Array(uvMap(TABLET_INDEX_WITH_PREFIX), uvMap(ROW_INDEX_WITH_PREFIX))

      val values: Array[UnversionedValue] = schema.fields
        .filter(field => !field.name.startsWith(STREAMING_SERVICE_KEY_COLUMNS_PREFIX))
        .map { field => uvMap(field.name) }

      val newRow = keys ++ values
      deserializeValues(newRow)
    }
  }

  private def getSchemaColumnsWithChangedServiceColumnsNames(ytSchema: TableSchema): TableSchema = {
    import scala.collection.JavaConverters._

    val columnsWithChangedServiceColumnsNames: List[ColumnSchema] = ytSchema.getColumns.asScala.map(column =>
      if (column.getName.equals("$tablet_index") || column.getName.equals("$row_index")) {
        val changedColumnName = if (column.getName.equals("$tablet_index")) {
          TABLET_INDEX_WITH_PREFIX
        } else {
          ROW_INDEX_WITH_PREFIX
        }

        new ColumnSchema.Builder(changedColumnName, column.getTypeV3)
          .setSortOrder(column.getSortOrder)
          .setAggregate(column.getAggregate)
          .setLock(column.getLock)
          .setGroup(column.getGroup)
          .setExpression(column.getExpression)
          .build()
      } else {
        column
      }
    ).toList

    new TableSchema.Builder(ytSchema).setColumns(columnsWithChangedServiceColumnsNames.asJava)
      .build()
  }
}
