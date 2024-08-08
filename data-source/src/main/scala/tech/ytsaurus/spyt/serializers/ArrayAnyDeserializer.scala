package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

import scala.collection.mutable


class ArrayAnyDeserializer(schema: StructType) extends WireDeserializer[Array[Any]](schema) {
  override def onCompleteRow(): Array[Any] = _values
}

object ArrayAnyDeserializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, ArrayAnyDeserializer]] =
    ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType, filters: Array[Filter] = Array.empty): ArrayAnyDeserializer = {
    deserializers.get().getOrElseUpdate(schema, new ArrayAnyDeserializer(schema))
  }
}
