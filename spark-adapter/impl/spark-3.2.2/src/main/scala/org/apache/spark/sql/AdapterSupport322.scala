package org.apache.spark.sql

import org.apache.spark.resource.ResourceProfile.ExecutorResourcesOrDefaults
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{NumericType, StructType}

// The sole purpose of this object is to increase visibility of some Spark package-private methods
object AdapterSupport322 {
  def getExecutorCores(execResources: Product): Int = execResources.asInstanceOf[ExecutorResourcesOrDefaults].cores
  def schemaToAttributes(schema: StructType): Seq[AttributeReference] = schema.toAttributes
  def castToLong(x: NumericType): Any => Any = { b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b) }
  def fromAttributes(attributes: Seq[Attribute]): StructType = StructType.fromAttributes(attributes)
}
