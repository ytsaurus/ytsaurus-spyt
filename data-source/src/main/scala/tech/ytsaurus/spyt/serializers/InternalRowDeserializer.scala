package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._


class InternalRowDeserializer(schema: StructType) extends WireDeserializer[InternalRow](schema) {
  override def onCompleteRow(): InternalRow = new GenericInternalRow(_values)
}

object InternalRowDeserializer {
  private val cache = new WireDeserializer.Cache(new InternalRowDeserializer(_))

  def getOrCreate(schema: StructType, filters: Array[Filter] = Array.empty): InternalRowDeserializer = {
    cache.getOrCreate(schema)
  }
}
