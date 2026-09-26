package tech.ytsaurus.spyt.example;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Example plugin that increments each non-null signed 64-bit value through a native Arrow function.
 *
 * <p>Register this provider by its fully qualified class name after distributing the companion JAR
 * and native library. Set the {@code library} option to the distributed library's filename, or use
 * the default {@code libspyt_native_udf.so}. The executor context resolves that name within the
 * submitting session, including files uploaded through Spark Connect. Set {@code delta} to a signed
 * 64-bit integer to change the increment; it defaults to one.
 */
public final class NativeIncrementProvider extends NativeFunctionProviderBase {

    /** Creates a provider for registration through its fully qualified class name. */
    public NativeIncrementProvider() {
        super("IncrementInt64", List.of(Field.nullable("input", new ArrowType.Int(64, true))),
                new ArrowType.Int(64, true));
    }

    /**
     * Validates the same increment constant during driver analysis and executor creation.
     *
     * @param options registration options
     * @return configured signed 64-bit delta, or one when omitted
     * @throws IllegalArgumentException if the value is not a signed 64-bit integer
     */
    @Override
    protected Long scalar(Map<String, String> options) {
        return Long.parseLong(options.getOrDefault("delta", "1"));
    }
}
