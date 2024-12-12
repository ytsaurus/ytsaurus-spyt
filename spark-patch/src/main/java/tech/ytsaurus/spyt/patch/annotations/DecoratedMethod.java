package tech.ytsaurus.spyt.patch.annotations;

import tech.ytsaurus.spyt.patch.MethodProcesor;

public @interface DecoratedMethod {
    Class<? extends MethodProcesor>[] baseMethodProcessors() default {};
}
