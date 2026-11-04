package tech.ytsaurus.spyt.patch.annotations;

import tech.ytsaurus.spyt.patch.MethodProcesor;

public @interface DecoratedMethod {

    /** Target method name; when empty, the decorator's name is used. Its signature must match the target. */
    String name() default "";

    Class<? extends MethodProcesor>[] baseMethodProcessors() default {};
}
