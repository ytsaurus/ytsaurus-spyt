package org.apache.spark.sql.catalyst.expressions

import tech.ytsaurus.spyt.patch.annotations.{Applicability, Decorate, OriginClass, PatchSource}

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.Cast")
@Applicability(from = "3.4.0")
@PatchSource("org.apache.spark.sql.catalyst.expressions.CastBaseDecorators")
class CastDecorators340
