package tech.ytsaurus.spyt.example;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.ytsaurus.spyt.format.columnar.ColumnarEvaluator;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionContext;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionDescriptor;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionProvider;

/**
 * Shares registration and native evaluator setup for deterministic functions with fixed Arrow schemas.
 * Subclasses declare their native symbol and field types, and override {@link #scalar} when needed.
 * Each concrete provider must expose a public no-argument constructor for SPYT registration.
 */
abstract class NativeFunctionProviderBase implements ColumnarFunctionProvider {

    private final String symbol;
    private final ColumnarFunctionDescriptor descriptor;

    /** Defines positional inputs and a single nullable output named value for the native function. */
    protected NativeFunctionProviderBase(String symbol, List<Field> inputs, ArrowType outputType) {
        this.symbol = symbol;
        this.descriptor = new ColumnarFunctionDescriptor(new Schema(inputs),
                new Schema(List.of(Field.nullable("value", outputType))), true);
    }

    /** Validates scalar options on the driver and returns the fixed schemas and determinism metadata. */
    @Override
    public final ColumnarFunctionDescriptor describe(Map<String, String> options) {
        scalar(options);
        return descriptor;
    }

    /** Resolves the library and creates a partition-local evaluator with the validated scalar argument. */
    @Override
    public final ColumnarEvaluator create(ColumnarFunctionContext context, Map<String, String> options) {
        Number scalar = scalar(options);
        String library = options.getOrDefault("library", "libspyt_native_udf.so");
        return new NativeArrowEvaluator(context.allocator(), context.resolveArtifact(library), symbol, scalar);
    }

    /** Returns the optional native scalar argument; override to parse and validate function-specific options. */
    protected Number scalar(Map<String, String> options) {
        return null;
    }
}
