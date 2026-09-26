package tech.ytsaurus.spyt.example;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Multiplies each non-null BIGINT by a constant factor and returns a nullable DOUBLE column.
 *
 * <p>Register by class name after distributing the companion JAR and {@code libspyt_native_udf.so}.
 * Set {@code factor} to a finite double (default {@code 2.0}), and optionally set {@code library}
 * to another artifact filename. Conversion to double follows normal floating-point rounding;
 * multiplication may produce infinity even though the configured factor is finite.
 */
public final class NativeScaleProvider extends NativeFunctionProviderBase {

    /** Creates a provider for registration through its fully qualified class name. */
    public NativeScaleProvider() {
        super("ScaleInt64ToDouble", List.of(Field.nullable("input", new ArrowType.Int(64, true))),
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    }

    /**
     * Parses the same scale constant during analysis and executor initialization.
     *
     * @param options registration options
     * @return finite factor, defaulting to two
     * @throws IllegalArgumentException if the supplied value is malformed or nonfinite
     */
    @Override
    protected Double scalar(Map<String, String> options) {
        double value = Double.parseDouble(options.getOrDefault("factor", "2.0"));
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("Native scale factor must be finite");
        }
        return value;
    }
}
