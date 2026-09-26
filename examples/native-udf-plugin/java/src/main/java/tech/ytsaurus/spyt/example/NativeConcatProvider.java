package tech.ytsaurus.spyt.example;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Concatenates two STRING columns, returning null whenever either input is null.
 *
 * <p>Register this provider by class name after uploading its companion JAR and
 * {@code libspyt_native_udf.so}. Override the {@code library} option to use another artifact filename.
 * The provider creates task-local evaluators that return newly allocated native Arrow buffers.
 */
public final class NativeConcatProvider extends NativeFunctionProviderBase {

    /** Creates a provider for registration through its fully qualified class name. */
    public NativeConcatProvider() {
        super("ConcatUtf8", List.of(Field.nullable("left", ArrowType.Utf8.INSTANCE),
                        Field.nullable("right", ArrowType.Utf8.INSTANCE)),
                ArrowType.Utf8.INSTANCE);
    }
}
