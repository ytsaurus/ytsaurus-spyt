package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._


class ArrayAnyDeserializer(schema: StructType) extends WireDeserializer[Array[Any]](schema) {
  override def onCompleteRow(): Array[Any] = _values
}

object ArrayAnyDeserializer {
  private val cache = new WireDeserializer.Cache(new ArrayAnyDeserializer(_))

  def getOrCreate(schema: StructType, filters: Array[Filter] = Array.empty): ArrayAnyDeserializer = {
    cache.getOrCreate(schema)
  }
}
