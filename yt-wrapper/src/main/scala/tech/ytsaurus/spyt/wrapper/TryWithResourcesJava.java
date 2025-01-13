package tech.ytsaurus.spyt.wrapper;

import scala.Function1;

public class TryWithResourcesJava {
    public static <C extends AutoCloseable, R> R apply(C autoCloseable, Function1<C, R> function) throws Exception {
        try (autoCloseable) {
            return function.apply(autoCloseable);
        }
    }
}
