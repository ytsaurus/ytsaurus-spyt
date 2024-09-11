package org.apache.spark.sql.v2

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.v2.YtScanBuilderBase.pushStructMetadata

abstract class YtScanBuilderBase(sba: ScanBuilderAdapter)
  extends FileScanBuilder(sba.sparkSession, sba.fileIndex, sba.dataSchema) {

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = pushStructMetadata(requiredSchema, sba.dataSchema)
  }

  override def build(): Scan = sba.build(readDataSchema(), readPartitionSchema())

}

object YtScanBuilderBase {
  private[v2] def pushStructMetadata(source: StructType, meta: StructType): StructType = {
    source.copy(fields = source.fields.map {
      f =>
        val opt = meta.fields.find(sf => sf.name == f.name)
        opt match {
          case None => f
          case Some(v) => f.copy(dataType = pushMetadata(f.dataType, v.dataType),
            metadata = v.metadata)
        }
    })
  }

  private def pushMapMetadata(source: MapType, meta: MapType): MapType = {
    source.copy(keyType = pushMetadata(source.keyType, meta.keyType),
      valueType = pushMetadata(source.valueType, meta.valueType))
  }

  private def pushArrayMetadata(source: ArrayType, meta: ArrayType): ArrayType = {
    source.copy(elementType = pushMetadata(source.elementType, meta.elementType))
  }

  private def pushMetadata(source: DataType, meta: DataType): DataType = {
    if (source.getClass != meta.getClass) {
      source
    } else {
      source match {
        case s: StructType => pushStructMetadata(s, meta.asInstanceOf[StructType])
        case m: MapType => pushMapMetadata(m, meta.asInstanceOf[MapType])
        case a: ArrayType => pushArrayMetadata(a, meta.asInstanceOf[ArrayType])
        case other => other
      }
    }
  }
}
