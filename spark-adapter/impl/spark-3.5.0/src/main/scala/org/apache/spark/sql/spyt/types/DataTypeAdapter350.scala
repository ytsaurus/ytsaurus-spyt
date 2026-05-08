package org.apache.spark.sql.spyt.types

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.{DataTypeUtils, PhysicalDataType, PhysicalNumericType}
import org.apache.spark.sql.types.{NumericType, StructType}
import tech.ytsaurus.spyt.{DataTypeAdapter, MinSparkVersion}

@MinSparkVersion("3.5.0")
class DataTypeAdapter350 extends DataTypeAdapter {

  override def castToLong(x: NumericType): Any => Any = {
    val numeric = PhysicalNumericType.numeric(x)
    b => numeric.toLong(b)
  }

  override def fromAttributes(attributes: Seq[Attribute]): StructType = {
    DataTypeUtils.fromAttributes(attributes)
  }
}
