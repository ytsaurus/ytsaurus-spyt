package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.CombinedTypeCoercionRule
import tech.ytsaurus.spyt.adapter.TypeSupport.{instance => ts}
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

@Subclass
@OriginClass("org.apache.spark.sql.catalyst.analysis.TypeCoercionBase$CombinedTypeCoercionRule")
class CombinedTypeCoercionRuleSpyt(base: TypeCoercionBase, rules: Seq[TypeCoercionRule])
  extends CombinedTypeCoercionRule(ts.typeCoercionRules ++ rules)
