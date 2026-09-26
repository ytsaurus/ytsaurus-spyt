package tech.ytsaurus.spyt.example;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Counts UTF-8 bytes in each non-null STRING and returns a nullable INT column.
 *
 * <p>Register this provider by class name after uploading its companion JAR and
 * {@code libspyt_native_udf.so}. Override the {@code library} option to use another artifact filename.
 * The provider creates task-local evaluators that return newly allocated native Arrow buffers.
 */
public final class NativeUtf8LengthProvider extends NativeFunctionProviderBase {

    /** Creates a provider for registration through its fully qualified class name. */
    public NativeUtf8LengthProvider() {
        super("Utf8Lengths", List.of(Field.nullable("input", ArrowType.Utf8.INSTANCE)),
                new ArrowType.Int(32, true));
    }
}
