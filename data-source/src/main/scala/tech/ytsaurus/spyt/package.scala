package tech.ytsaurus

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings._
import tech.ytsaurus.spyt.wrapper.client.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.config._
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.config.ConfigEntry
import tech.ytsaurus.spyt.serializers.{SchemaConverter, YtLogicalType}
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

package object spyt {
  lazy val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(SparkSession.getDefaultSession.get))

  private def normalizePath(path: String): String = {
    if (path.startsWith("//")) path.drop(1) else path
  }

  implicit class YtReader(reader: DataFrameReader) {
    def yt(paths: String*): DataFrame = reader.format("yt").load(paths: _*)

    def schemaHint(schemaHint: StructType): DataFrameReader = {
      reader.options(SchemaConverter.serializeSchemaHint(schemaHint))
    }

    def schemaHint(structField: StructField, structFields: StructField*): DataFrameReader = {
      schemaHint(StructType(structField +: structFields))
    }

    def schemaHint(field: (String, DataType), fields: (String, DataType)*): DataFrameReader = {
      schemaHint(
        StructField(field._1, field._2),
        fields.map { case (name, dataType) => StructField(name, dataType) }: _*
      )
    }

    def enableArrow(enable: Boolean): DataFrameReader = {
      reader.option(ArrowEnabled, enable)
    }

    def enableArrow: DataFrameReader = {
      enableArrow(true)
    }

    def disableArrow: DataFrameReader = {
      enableArrow(false)
    }

    def transaction(id: String): DataFrameReader = {
      reader.option(Transaction, id)
    }

    def timestamp(ts: Long): DataFrameReader = {
      reader.option(Timestamp, ts)
    }

    def option[T](entry: ConfigEntry[T], value: T): DataFrameReader = {
      val stringValue = entry.set(value)
      reader.option(entry.name, stringValue)
    }
  }

  implicit class YtWriter[T](writer: DataFrameWriter[T]) {
    def yt(path: String): Unit = writer.format("yt").save(normalizePath(path))

    def option[S](config: ConfigEntry[S], value: S): DataFrameWriter[T] = {
      val stringValue = config.set(value)
      writer.option(config.name, stringValue)
    }

    def optimizeFor(optimizeMode: OptimizeMode): DataFrameWriter[T] = {
      writer.option(OptimizeFor, optimizeMode.name)
    }

    def transaction(id: String): DataFrameWriter[T] = {
      writer.option(WriteTransaction, id)
    }

    def optimizeFor(optimizeMode: String): DataFrameWriter[T] = {
      optimizeFor(OptimizeMode.fromName(optimizeMode))
    }

    def sortedBy(cols: String*): DataFrameWriter[T] = {
      writer.option(SortColumns, cols)
    }

    def sortedByUniqueKeys(cols: String*): DataFrameWriter[T] = {
      writer.sortedBy(cols:_*).uniqueKeys
    }

    def uniqueKeys: DataFrameWriter[T] = {
      writer.option(UniqueKeys, true)
    }

    def schemaHint(schemaHint: Map[String, YtLogicalType]): DataFrameWriter[T] = {
      writer.option(WriteSchemaHint, schemaHint)
    }

    def schemaHint(field: (String, YtLogicalType), fields: (String, YtLogicalType)*): DataFrameWriter[T] = {
      schemaHint(fields.toMap + field)
    }
  }

  implicit class YtDataset[T](df: Dataset[T]) {
    def withYsonColumn(name: String, column: Column): DataFrame = {
      val colSchema = df.withColumn(name, column).schema(name)
      val metadata = new MetadataBuilder()
        .withMetadata(colSchema.metadata)
        .putBoolean("skipNulls", true)
        .build()
      val newColumn = column.as(name, metadata)
      df.withColumn(name, newColumn)
    }

    def selectAs[S](implicit encoder: Encoder[S]): Dataset[S] = {
      val names = encoder.schema.fieldNames
      df.select(names.head, names.tail: _*).as[S]
    }
  }

}
