package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import tech.ytsaurus.spyt.{MinSparkVersion, SchemaAdapter}

@MinSparkVersion("3.5.0")
class SchemaAdapter350 extends SchemaAdapter {
  override def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    DataTypeUtils.toAttributes(schema)
  }
}
