package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveLeafEncoder
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.types.UInt64Long

case object UInt64Encoder extends PrimitiveLeafEncoder[UInt64Long](ts.uInt64DataType)
