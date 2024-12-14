package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{NumericType, StructType}
import tech.ytsaurus.spyt.{DataTypeAdapter, MinSparkVersion}

@MinSparkVersion("3.2.2")
class DataTypeAdapter322 extends DataTypeAdapter {

  override def castToLong(x: NumericType): Any => Any = {
    b => x.numeric.asInstanceOf[Numeric[Any]].toLong(b)
  }

  override def fromAttributes(attributes: Seq[Attribute]): StructType = {
    StructType.fromAttributes(attributes)
  }
}
