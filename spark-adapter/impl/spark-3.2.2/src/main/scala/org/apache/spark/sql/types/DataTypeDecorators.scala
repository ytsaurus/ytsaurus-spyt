package org.apache.spark.sql.types

import tech.ytsaurus.spyt.adapter.TypeSupport
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.sql.types.DataType$")
object DataTypeDecorators {

  @DecoratedMethod
  private def nameToType(name: String): DataType = name match {
    case "uint64" => TypeSupport.instance.uInt64DataType
    case _ => __nameToType(name)
  }

  private def __nameToType(name: String): DataType = ???
}
