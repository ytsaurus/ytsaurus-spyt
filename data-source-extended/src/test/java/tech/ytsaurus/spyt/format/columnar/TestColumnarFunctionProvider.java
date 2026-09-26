package tech.ytsaurus.spyt.format.columnar;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Pure Java Arrow plugin used to exercise SPYT execution without a native library.
 *
 * <p>Register this class with {@code REGISTER COLUMNAR FUNCTION} in local Spark tests. By default it
 * adds two nullable long columns and an optional {@code offset}. The {@code mode} option selects
 * struct output, retained input buffers, invalid output, deliberate lifecycle failures, or a
 * per-evaluator offset for checking independent nondeterministic calls. An {@code artifact}
 * option reads the offset from a distributed file to test session-aware artifact resolution.
 * Lifecycle counters and retained output references let tests verify resource cleanup; call
 * {@link #reset()} only after the preceding query has completed.
 */
public class TestColumnarFunctionProvider implements ColumnarFunctionProvider {

    /** Number of evaluators created since the last reset. */
    public static final AtomicInteger CREATED = new AtomicInteger();
    /** Number of evaluators closed since the last reset. */
    public static final AtomicInteger CLOSED = new AtomicInteger();
    /** Number of nonempty identity outputs verified to share their input data buffer. */
    public static final AtomicInteger SHARED_OUTPUTS = new AtomicInteger();
    /** Partition allocators retained to verify cleanup, including failures during evaluator creation. */
    public static final ConcurrentLinkedQueue<BufferAllocator> ALLOCATORS = new ConcurrentLinkedQueue<>();
    /** Output roots retained for assertions about buffer release after Spark closes them. */
    public static final ConcurrentLinkedQueue<VectorSchemaRoot> OUTPUTS = new ConcurrentLinkedQueue<>();

    /**
     * Builds a nullable signed 64-bit field for the provider's input and output schemas.
     *
     * @param name field name in the batch schema
     * @return the field definition, without allocating vector buffers
     */
    private static Field longField(String name) {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
    }

    /**
     * Declares two nullable long inputs and either a long result or a struct containing sum and product.
     *
     * @param options registration options; {@code mode=struct} selects the struct result,
     *                {@code mode=uint64} adds unsigned 64-bit inputs with an unsigned result
     * @return the function descriptor used during SQL analysis; nondeterministic mode disables reuse
     */
    @Override
    public ColumnarFunctionDescriptor describe(Map<String, String> options) {
        String mode = options.getOrDefault("mode", "normal");
        if (mode.equals("uint64")) {
            FieldType type = FieldType.nullable(new ArrowType.Int(64, false));
            return new ColumnarFunctionDescriptor(
                    new Schema(Arrays.asList(new Field("left", type, null), new Field("right", type, null))),
                    new Schema(Collections.singletonList(new Field("result", type, null))), true);
        }
        boolean requiredChildren = mode.equals("null-parent");
        Field sum = requiredChildren
                ? new Field("sum", FieldType.notNullable(new ArrowType.Int(64, true)), null)
                : longField("sum");
        Field result = (mode.equals("struct") || requiredChildren)
                ? new Field("result", FieldType.nullable(ArrowType.Struct.INSTANCE),
                        Arrays.asList(sum, longField("product")))
                : longField("result");
        return new ColumnarFunctionDescriptor(
                new Schema(Arrays.asList(longField("left"), longField("right"))),
                new Schema(Collections.singletonList(result)), !mode.equals("nondeterministic"));
    }

    /**
     * Creates a partition evaluator and resolves any configured offset artifact before evaluation.
     *
     * @param context executor allocator and session-aware artifact resolver supplied by SPYT
     * @param options registration options selecting the offset and test behavior
     * @return an evaluator whose lifecycle can be checked through the static counters
     * @throws IllegalStateException for creation-failure mode or if an artifact cannot be read as a long
     */
    @Override
    public ColumnarEvaluator create(ColumnarFunctionContext context, Map<String, String> options) {
        ALLOCATORS.add(context.allocator());
        if (options.getOrDefault("mode", "normal").equals("create-fail")) {
            throw new IllegalStateException("test provider creation failure");
        }
        long configuredOffset = Long.parseLong(options.getOrDefault("offset", "0"));
        if (options.containsKey("artifact")) {
            try {
                configuredOffset = Long.parseLong(Files.readString(
                        Paths.get(context.resolveArtifact(options.get("artifact")))).trim());
            } catch (Exception exception) {
                throw new IllegalStateException("Cannot read test artifact", exception);
            }
        }
        return new TestColumnarEvaluator(context.allocator(), describe(options).outputSchema(),
                options.getOrDefault("mode", "normal"), configuredOffset, CREATED.incrementAndGet());
    }

    /**
     * Clears lifecycle observations before a test query. This method does not release Arrow resources;
     * wait for previous queries to finish and close their outputs before invoking it.
     */
    public static void reset() {
        CREATED.set(0);
        CLOSED.set(0);
        SHARED_OUTPUTS.set(0);
        OUTPUTS.clear();
        ALLOCATORS.clear();
    }
}

/**
 * Evaluates the modes selected by {@link TestColumnarFunctionProvider} for columnar execution tests.
 * The provider supplies immutable configuration; returned roots remain owned by Spark.
 */
class TestColumnarEvaluator implements ColumnarEvaluator {

    private final BufferAllocator allocator;
    private final Schema outputSchema;
    private final String mode;
    private final long offset;
    private final int evaluatorId;
    private boolean closed;

    /** Creates an evaluator with the provider's resolved offset, output schema, and lifecycle identifier. */
    TestColumnarEvaluator(BufferAllocator allocator, Schema outputSchema, String mode, long offset, int evaluatorId) {
        this.allocator = allocator;
        this.outputSchema = outputSchema;
        this.mode = mode;
        this.offset = offset;
        this.evaluatorId = evaluatorId;
    }

    /**
     * Produces the selected test output while borrowing the input batch.
     * Identity mode retains input buffers through an Arrow transfer; other successful modes
     * allocate output with the executor allocator. The caller owns and must close returned roots.
     *
     * @param input batch containing the two declared long columns
     * @return a caller-owned result, deliberately malformed in the invalid-output modes
     * @throws IllegalStateException for failure mode or an unexpected identity buffer copy
     */
    @Override
    public VectorSchemaRoot evaluate(VectorSchemaRoot input) {
        if (mode.equals("fail")) {
            throw new IllegalStateException("test provider failure");
        }
        Schema schema = mode.equals("wrong-schema")
                ? new Schema(Collections.singletonList(new Field("result",
                        FieldType.nullable(new ArrowType.Utf8()), null)))
                : outputSchema;
        if (mode.equals("identity")) {
            FieldVector source = input.getVector(0);
            FieldVector result = schema.getFields().get(0).createVector(source.getAllocator());
            TransferPair transfer = source.makeTransferPair(result);
            transfer.splitAndTransfer(0, input.getRowCount());
            VectorSchemaRoot output = new VectorSchemaRoot(schema,
                    Collections.singletonList(result), input.getRowCount());
            TestColumnarFunctionProvider.OUTPUTS.add(output);
            if (input.getRowCount() > 0) {
                if (source.getDataBuffer().memoryAddress() != result.getDataBuffer().memoryAddress()) {
                    throw new IllegalStateException("Identity output did not retain the input buffer");
                }
                TestColumnarFunctionProvider.SHARED_OUTPUTS.incrementAndGet();
            }
            return output;
        }
        VectorSchemaRoot output = VectorSchemaRoot.create(schema, allocator);
        TestColumnarFunctionProvider.OUTPUTS.add(output);
        output.allocateNew();
        int count = input.getRowCount() + (mode.equals("wrong-count") ? 1 : 0);
        if (mode.equals("struct") || mode.equals("null-parent")) {
            BigIntVector left = (BigIntVector) input.getVector(0);
            BigIntVector right = (BigIntVector) input.getVector(1);
            StructVector result = (StructVector) output.getVector(0);
            BigIntVector sum = result.getChild("sum", BigIntVector.class);
            BigIntVector product = result.getChild("product", BigIntVector.class);
            for (int row = 0; row < input.getRowCount(); row++) {
                if (left.isNull(row) || right.isNull(row) || (mode.equals("null-parent") && row == 0)) {
                    result.setNull(row);
                } else {
                    result.setIndexDefined(row);
                    sum.setSafe(row, left.get(row) + right.get(row));
                    product.setSafe(row, left.get(row) * right.get(row));
                }
            }
        } else if (mode.equals("uint64")) {
            UInt8Vector left = (UInt8Vector) input.getVector(0);
            UInt8Vector right = (UInt8Vector) input.getVector(1);
            UInt8Vector result = (UInt8Vector) output.getVector(0);
            for (int row = 0; row < input.getRowCount(); row++) {
                if (left.isNull(row) || right.isNull(row)) {
                    result.setNull(row);
                } else {
                    result.setSafe(row, left.get(row) + right.get(row));
                }
            }
        } else if (!mode.equals("wrong-schema")) {
            BigIntVector left = (BigIntVector) input.getVector(0);
            BigIntVector right = (BigIntVector) input.getVector(1);
            BigIntVector result = (BigIntVector) output.getVector(0);
            for (int row = 0; row < input.getRowCount(); row++) {
                if (left.isNull(row) || right.isNull(row)) {
                    result.setNull(row);
                } else {
                    result.setSafe(row, left.get(row) + right.get(row) + offset
                            + (mode.equals("nondeterministic") ? evaluatorId : 0));
                }
            }
        }
        output.setRowCount(count);
        return output;
    }

    /**
     * Records evaluator cleanup without closing output roots owned by Spark.
     *
     * @throws IllegalStateException in close-failure mode or if Spark closes this evaluator more than once
     */
    @Override
    public void close() {
        if (closed) {
            throw new IllegalStateException("Evaluator closed twice");
        }
        closed = true;
        TestColumnarFunctionProvider.CLOSED.incrementAndGet();
        if (mode.equals("close-fail")) {
            throw new IllegalStateException("test evaluator closing failure");
        }
    }
}
