package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import tech.ytsaurus.spyt.{MinSparkVersion, SchemaAdapter}

@MinSparkVersion("3.2.2")
class SchemaAdapter322 extends SchemaAdapter {
  override def schemaToAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.toAttributes
  }
}
