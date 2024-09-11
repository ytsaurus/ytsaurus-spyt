package tech.ytsaurus.spyt.patch.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
public @interface Applicability {
    /**
     * Spark version from which this patch is applicable, inclusive.
     */
    String from() default "3.2.2";

    /**
     * Spark version to which this patch is applicable, inclusive. If empty string, then the upper version is unbounded.
     */
    String to() default "";
}
