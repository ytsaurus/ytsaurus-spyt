package tech.ytsaurus.spyt.example;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.ytsaurus.spyt.format.columnar.ColumnarEvaluator;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionContext;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionDescriptor;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Exercises the example's allocating C Data functions through the public SPYT provider contract.
 *
 * <p>Run {@code ya make -r -t examples/native-udf-plugin} from the SPYT project root.
 * The test manifest builds and locates the native library automatically.
 * Inputs remain owned by the tests; result roots are always closed before their evaluators.
 */
public class NativeArrowFunctionsTest {

    /** Values covering empty text, ASCII, multibyte code points, and null propagation. */
    private static final String[] TEXT = {"", "Arrow", "é", "😀", null, "a\u0000b"};

    /**
     * Builds a test context that verifies the default distributed filename and borrows an allocator.
     *
     * @param allocator test-owned allocator which outlives the evaluator
     * @return executor services backed by the configured locally built native library
     */
    private ColumnarFunctionContext context(RootAllocator allocator) {
        String library = System.getProperty("spyt.test.nativeUdfLibrary");
        assumeTrue(library != null, "Native library path is not configured");
        return new ColumnarFunctionContext() {

            /** @return the allocator owned and closed by the enclosing test */
            @Override
            public RootAllocator allocator() {
                return allocator;
            }

            /**
             * Resolves the example's default filename to the test fixture.
             *
             * @param name distributed filename requested by the provider
             * @return the configured local native library path
             */
            @Override
            public String resolveArtifact(String name) {
                assertEquals("libspyt_native_udf.so", name);
                return library;
            }
        };
    }

    /** Supplies default and fractional scaling for empty, small, and larger nullable or all-valid batches. */
    private static Stream<Arguments> scalingCases() {
        return Stream.of(Map.<String, String>of(), Map.of("factor", "-0.5"))
                .flatMap(options -> Stream.of(0, 1, 5, 17)
                        .flatMap(size -> Stream.of(false, true)
                                .map(nullable -> Arguments.of(options, size, nullable))));
    }

    /**
     * Verifies scaling across separate scan/task allocators and repeated evaluations without changing input.
     *
     * @param options provider options selecting the scale factor
     * @param size number of input rows
     * @param nullable whether to include null input values
     */
    @ParameterizedTest(name = "options={0}, size={1}, nullable={2}")
    @MethodSource("scalingCases")
    public void scalesLongsIntoDoublesWithoutChangingInput(Map<String, String> options, int size, boolean nullable) {
        NativeScaleProvider provider = new NativeScaleProvider();
        long[] values = {Long.MIN_VALUE, -3, 0, 7, Long.MAX_VALUE};
        double factor = Double.parseDouble(options.getOrDefault("factor", "2.0"));
        ColumnarFunctionDescriptor descriptor = provider.describe(options);
        try (RootAllocator inputAllocator = new RootAllocator();
             RootAllocator allocator = new RootAllocator();
             ColumnarEvaluator evaluator = provider.create(context(allocator), options)) {
            for (int iteration = 0; iteration < 3; iteration++) {
                try (VectorSchemaRoot input =
                        VectorSchemaRoot.create(descriptor.inputSchema(), inputAllocator)) {
                    BigIntVector source = (BigIntVector) input.getVector(0);
                    source.allocateNew(size);
                    for (int row = 0; row < size; row++) {
                        if (nullable && row % 3 == 0) {
                            source.setNull(row);
                        } else {
                            source.set(row, values[row % values.length]);
                        }
                    }
                    input.setRowCount(size);
                    long baseline = allocator.getAllocatedMemory();
                    long inputBaseline = inputAllocator.getAllocatedMemory();
                    try (VectorSchemaRoot output = evaluator.evaluate(input)) {
                        assertEquals(descriptor.outputSchema(), output.getSchema());
                        assertEquals(size, output.getRowCount());
                        Float8Vector result = (Float8Vector) output.getVector(0);
                        for (int row = 0; row < size; row++) {
                            boolean isNull = nullable && row % 3 == 0;
                            assertEquals(isNull, source.isNull(row));
                            assertEquals(isNull, result.isNull(row));
                            if (!isNull) {
                                assertEquals(values[row % values.length], source.get(row));
                                assertEquals((double) values[row % values.length] * factor,
                                        result.get(row), 0.0);
                            }
                        }
                    }
                    assertEquals(baseline, allocator.getAllocatedMemory());
                    assertEquals(inputBaseline, inputAllocator.getAllocatedMemory());
                }
                assertEquals(0L, allocator.getAllocatedMemory());
                assertEquals(0L, inputAllocator.getAllocatedMemory());
            }
        }
    }

    /** Supplies empty and nonempty text batches with and without nulls. */
    private static Stream<Arguments> textCases() {
        return Stream.of(0, 1, 6, 19)
                .flatMap(size -> Stream.of(false, true).map(nullable -> Arguments.of(size, nullable)));
    }

    /**
     * Verifies UTF8-to-INT32 measures bytes, preserving Unicode, embedded zeroes, and nulls.
     *
     * @param size number of input rows
     * @param nullable whether to include null input values
     */
    @ParameterizedTest(name = "size={0}, nullable={1}")
    @MethodSource("textCases")
    public void returnsUtf8ByteLengths(int size, boolean nullable) {
        checkTextFunction(new NativeUtf8LengthProvider(), false, size, nullable);
    }

    /**
     * Verifies two UTF8 inputs produce newly allocated UTF8 output and propagate either null.
     *
     * @param size number of input rows
     * @param nullable whether to include null input values
     */
    @ParameterizedTest(name = "size={0}, nullable={1}")
    @MethodSource("textCases")
    public void concatenatesTwoUtf8Columns(int size, boolean nullable) {
        checkTextFunction(new NativeConcatProvider(), true, size, nullable);
    }

    /**
     * Exercises variable-width input and both variable-width and fixed-width native output.
     *
     * @param provider text function implementation to instantiate through its public interface
     * @param concatenate whether to supply a second input and expect concatenated text
     * @param size number of input rows
     * @param nullable whether to include null input values
     */
    private void checkTextFunction(ColumnarFunctionProvider provider, boolean concatenate, int size, boolean nullable) {
        ColumnarFunctionDescriptor descriptor = provider.describe(Map.of());
        try (RootAllocator leftAllocator = new RootAllocator();
             RootAllocator rightAllocator = new RootAllocator();
             RootAllocator allocator = new RootAllocator();
             ColumnarEvaluator evaluator = provider.create(context(allocator), Map.of())) {
            for (int iteration = 0; iteration < 3; iteration++) {
                var fields = descriptor.inputSchema().getFields();
                var vectors = java.util.stream.IntStream.range(0, fields.size())
                        .mapToObj(i -> fields.get(i).createVector(i == 0 ? leftAllocator : rightAllocator))
                        .toList();
                try (VectorSchemaRoot input = new VectorSchemaRoot(descriptor.inputSchema(), vectors, 0)) {
                    input.allocateNew();
                    for (int column = 0; column < input.getFieldVectors().size(); column++) {
                        VarCharVector vector = (VarCharVector) input.getVector(column);
                        for (int row = 0; row < size; row++) {
                            String value = text(row, column, nullable);
                            if (value == null) {
                                vector.setNull(row);
                            } else {
                                vector.setSafe(row, value.getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    }
                    input.setRowCount(size);
                    long baseline = allocator.getAllocatedMemory();
                    long leftBaseline = leftAllocator.getAllocatedMemory();
                    long rightBaseline = rightAllocator.getAllocatedMemory();
                    try (VectorSchemaRoot output = evaluator.evaluate(input)) {
                        assertEquals(descriptor.outputSchema(), output.getSchema());
                        assertEquals(size, output.getRowCount());
                        for (int row = 0; row < size; row++) {
                            String left = text(row, 0, nullable);
                            String right = concatenate ? text(row, 1, nullable) : "";
                            for (int column = 0; column < input.getFieldVectors().size(); column++) {
                                assertEquals(text(row, column, nullable),
                                        readText((VarCharVector) input.getVector(column), row));
                            }
                            assertEquals(left == null || right == null, output.getVector(0).isNull(row));
                            if (left != null && right != null) {
                                if (concatenate) {
                                    assertEquals(left + right,
                                            readText((VarCharVector) output.getVector(0), row));
                                } else {
                                    assertEquals(left.getBytes(StandardCharsets.UTF_8).length,
                                            ((IntVector) output.getVector(0)).get(row));
                                }
                            }
                        }
                    }
                    assertEquals(baseline, allocator.getAllocatedMemory());
                    assertEquals(leftBaseline, leftAllocator.getAllocatedMemory());
                    assertEquals(rightBaseline, rightAllocator.getAllocatedMemory());
                }
                assertEquals(0L, allocator.getAllocatedMemory());
                assertEquals(0L, leftAllocator.getAllocatedMemory());
                assertEquals(0L, rightAllocator.getAllocatedMemory());
            }
        }
    }

    /**
     * Selects independent left/right fixture values, replacing nulls for all-valid batches.
     *
     * @param row input row index
     * @param column input column index
     * @param nullable whether the fixture may contain nulls
     * @return expected input text, possibly null
     */
    private static String text(int row, int column, boolean nullable) {
        String value = TEXT[(row + column * 2) % TEXT.length];
        return !nullable && value == null ? "replacement" : value;
    }

    /**
     * Reads a nullable UTF8 value without converting a null slot to text.
     *
     * @param vector input or output vector to inspect
     * @param row row index
     * @return decoded text, or null for an invalid slot
     */
    private static String readText(VarCharVector vector, int row) {
        return vector.isNull(row) ? null : new String(vector.get(row), StandardCharsets.UTF_8);
    }

    /** Verifies native schema errors release exports and do not prevent reuse or safe closure. */
    @Test
    public void recoversAfterWrongInputTypeAndRejectsUseAfterClose() {
        NativeScaleProvider provider = new NativeScaleProvider();
        try (RootAllocator allocator = new RootAllocator();
             ColumnarEvaluator evaluator = provider.create(context(allocator), Map.of());
             VectorSchemaRoot wrong = VectorSchemaRoot.of(new VarCharVector("input", allocator));
             VectorSchemaRoot valid = VectorSchemaRoot.create(provider.describe(Map.of()).inputSchema(), allocator)) {
            wrong.allocateNew();
            ((VarCharVector) wrong.getVector(0)).setSafe(0, "invalid".getBytes(StandardCharsets.UTF_8));
            wrong.setRowCount(1);
            valid.allocateNew();
            ((BigIntVector) valid.getVector(0)).set(0, 21L);
            valid.setRowCount(1);
            long baseline = allocator.getAllocatedMemory();
            assertThrows(IllegalStateException.class, () -> evaluator.evaluate(wrong));
            assertEquals(baseline, allocator.getAllocatedMemory());
            assertEquals("invalid", readText((VarCharVector) wrong.getVector(0), 0));
            try (VectorSchemaRoot output = evaluator.evaluate(valid)) {
                assertEquals(42.0, ((Float8Vector) output.getVector(0)).get(0), 0.0);
            }
            assertEquals(baseline, allocator.getAllocatedMemory());
            evaluator.close();
            assertThrows(IllegalStateException.class, () -> evaluator.evaluate(valid));
            assertEquals(baseline, allocator.getAllocatedMemory());
        }
    }
}
