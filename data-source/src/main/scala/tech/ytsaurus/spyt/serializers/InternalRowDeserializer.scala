package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

import scala.collection.mutable


class InternalRowDeserializer(schema: StructType) extends WireDeserializer[InternalRow](schema) {
  override def onCompleteRow(): InternalRow = new GenericInternalRow(_values)
}

object InternalRowDeserializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, InternalRowDeserializer]] =
    ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType, filters: Array[Filter] = Array.empty): InternalRowDeserializer = {
    deserializers.get().getOrElseUpdate(schema, new InternalRowDeserializer(schema))
  }
}
