package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.StreamingUtils.STREAMING_SERVICE_KEY_COLUMNS_PREFIX
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.client.rows.{QueueRowset, UnversionedRowset, UnversionedValue}

class UnversionedRowsetDeserializer(schema: StructType) {
  private val deserializer = InternalRowDeserializer.getOrCreate(schema)

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
    values.zipWithIndex.foreach { case (value, index) =>
      deserializer.setId(index)
      deserializer.setType(value.getType)
      deserializeValue(value)
    }
    deserializer.onCompleteRow()
  }

  def deserializeRowset(rowset: UnversionedRowset): Iterator[InternalRow] = {
    import scala.collection.JavaConverters._
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

    rowset.getRows.asScala.iterator.map { row =>
      val uvMap = row.getValues.asScala.map { value =>
        rowset.getSchema.getColumnName(value.getId) -> value
      }.toMap

      val keys = Array(uvMap("$tablet_index"), uvMap("$row_index"))

      val values: Array[UnversionedValue] = schema.fields
        .filter(field => !field.name.startsWith(STREAMING_SERVICE_KEY_COLUMNS_PREFIX))
        .map { field => uvMap(field.name) }

      val newRow = keys ++ values
      deserializeValues(newRow)
    }
  }
}
