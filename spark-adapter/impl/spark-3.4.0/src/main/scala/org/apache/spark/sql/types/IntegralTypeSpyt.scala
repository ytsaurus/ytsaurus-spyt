package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalLongType}
import tech.ytsaurus.spyt.patch.annotations.{Applicability, OriginClass, Subclass}

@Subclass
@OriginClass("org.apache.spark.sql.types.IntegralType")
@Applicability(from = "3.4.0")
private[sql] abstract class IntegralTypeSpyt extends IntegralType {
  override private[sql] def physicalDataType: PhysicalDataType = {
    // We're hooking to uint64 here instead of UInt64Type because this method was introduced in Spark 3.4.0 to
    // preserve backward compatibility with older Spark versions
    if (catalogString == "uint64") PhysicalLongType else super.physicalDataType
  }
}
