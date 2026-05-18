package org.apache.spark.util

import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, DecoratedMethod, OriginClass}

@Decorate
@OriginClass("org.apache.spark.util.Utils$")
@Applicability(to = "3.4.0")
private[spark] object UtilsDecorators {

  @DecoratedMethod
  def localHostName(): String = {
    tech.ytsaurus.spyt.Utils.addBracketsIfIpV6Host(__localHostName())
  }

  def __localHostName(): String = ???
}
