package tech.ytsaurus.spyt.example;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.spyt.format.columnar.ColumnarEvaluator;
import tech.ytsaurus.spyt.format.columnar.ColumnarFunctionContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Standalone plugin tests for native results, Arrow ownership, and executor artifact resolution.
 *
 * <p>Run {@code ya make -r -t examples/native-udf-plugin} from the SPYT project root.
 * The test manifest builds the native library and supplies its path through
 * {@code spyt.test.nativeUdfLibrary}.
 */
public class NativeIncrementTest {

    /**
     * Obtains the native fixture path, skipping the current test when none was configured.
     *
     * @return the local path provided by the standalone build's native-library test property
     */
    private String library() {
        String path = System.getProperty("spyt.test.nativeUdfLibrary");
        assumeTrue(path != null, "Native library path is not configured");
        return path;
    }

    /** Supplies empty and nonempty increment batches with and without nulls. */
    private static Stream<Arguments> batchCases() {
        return Stream.of(0, 1, 3, 9, 64)
                .flatMap(size -> Stream.of(false, true).map(nullable -> Arguments.of(size, nullable)));
    }

    /**
     * Verifies reusable evaluation for empty, nullable, and non-nullable batches without changing
     * input values or retaining output allocations after their roots are closed.
     *
     * @param size number of input rows
     * @param nullable whether to include null input values
     */
    @ParameterizedTest(name = "size={0}, nullable={1}")
    @MethodSource("batchCases")
    public void preservesInputAndNullsAcrossRepeatedBatches(int size, boolean nullable) {
        String library = library();
        try (RootAllocator inputAllocator = new RootAllocator();
             RootAllocator allocator = new RootAllocator();
             NativeArrowEvaluator evaluator = new NativeArrowEvaluator(allocator, library, "IncrementInt64", 1L)) {
            for (int iteration = 0; iteration < 3; iteration++) {
                try (VectorSchemaRoot input = VectorSchemaRoot.of(new BigIntVector("input", inputAllocator))) {
                    BigIntVector source = (BigIntVector) input.getVector(0);
                    source.allocateNew(size);
                    for (int row = 0; row < size; row++) {
                        source.set(row, row - 32L);
                        if (nullable && row % 3 == 0) {
                            source.setNull(row);
                        }
                    }
                    input.setRowCount(size);
                    long baseline = allocator.getAllocatedMemory();
                    long inputBaseline = inputAllocator.getAllocatedMemory();
                    try (VectorSchemaRoot output = evaluator.evaluate(input)) {
                        assertEquals(size, output.getRowCount());
                        BigIntVector result = (BigIntVector) output.getVector(0);
                        for (int row = 0; row < size; row++) {
                            assertEquals(source.isNull(row), result.isNull(row));
                            if (!source.isNull(row)) {
                                assertEquals(row - 32L, source.get(row));
                                assertEquals(row - 31L, result.get(row));
                            }
                        }
                    }
                    assertEquals(baseline, allocator.getAllocatedMemory());
                    assertEquals(inputBaseline, inputAllocator.getAllocatedMemory());
                }
                assertEquals(0L, allocator.getAllocatedMemory());
            }
        }
    }

    /**
     * Verifies overflow releases partial output, permits later evaluation, and that closing the
     * evaluator rejects subsequent calls without releasing the caller's input buffers.
     */
    @Test
    public void cleansFailedOutputAndCanBeReused() {
        String library = library();
        try (RootAllocator allocator = new RootAllocator();
             NativeArrowEvaluator evaluator = new NativeArrowEvaluator(allocator, library, "IncrementInt64", 1L);
             VectorSchemaRoot input = VectorSchemaRoot.of(new BigIntVector("input", allocator))) {
            BigIntVector source = (BigIntVector) input.getVector(0);
            source.allocateNew(2);
            source.set(0, 42L);
            source.set(1, Long.MAX_VALUE);
            input.setRowCount(2);
            long baseline = allocator.getAllocatedMemory();
            assertThrows(ArithmeticException.class, () -> evaluator.evaluate(input));
            assertEquals(baseline, allocator.getAllocatedMemory());
            assertEquals(42L, source.get(0));
            assertEquals(Long.MAX_VALUE, source.get(1));
            source.set(1, Long.MIN_VALUE);
            try (VectorSchemaRoot output = evaluator.evaluate(input)) {
                assertEquals(43L, ((BigIntVector) output.getVector(0)).get(0));
                assertEquals(Long.MIN_VALUE + 1L, ((BigIntVector) output.getVector(0)).get(1));
            }
            evaluator.close();
            assertThrows(IllegalStateException.class, () -> evaluator.evaluate(input));
            assertEquals(baseline, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies provider creation delegates the configured filename to executor artifact resolution
     * and applies positive, negative, zero, and boundary deltas with overflow and allocation checks.
     *
     * @param delta increment to apply; one exercises the default provider option
     */
    @ParameterizedTest
    @ValueSource(longs = {1, 10, -10, 0, Long.MIN_VALUE, Long.MAX_VALUE})
    public void resolvesLibraryThroughExecutorContext(long delta) {
        String library = library();
        String filename = Paths.get(library).getFileName().toString();
        AtomicReference<String> requested = new AtomicReference<>();
        try (RootAllocator allocator = new RootAllocator()) {
            ColumnarFunctionContext context = new ColumnarFunctionContext() {

                /**
                 * Supplies the test-owned allocator, which outlives the evaluator.
                 *
                 * @return the allocator used to detect unreleased plugin allocations
                 */
                @Override
                public RootAllocator allocator() {
                    return allocator;
                }

                /**
                 * Records the requested artifact name and maps it to the configured local fixture.
                 *
                 * @param name distributed filename requested by the provider
                 * @return the native test library's local path
                 */
                @Override
                public String resolveArtifact(String name) {
                    requested.set(name);
                    return library;
                }
            };
            NativeIncrementProvider provider = new NativeIncrementProvider();
            Map<String, String> options = delta == 1 ? Map.of("library", filename)
                    : Map.of("library", filename, "delta", Long.toString(delta));
            provider.describe(options);
            try (ColumnarEvaluator evaluator = provider.create(context, options);
                 VectorSchemaRoot input = VectorSchemaRoot.of(new BigIntVector("input", allocator))) {
                assertEquals(filename, requested.get());
                BigIntVector source = (BigIntVector) input.getVector(0);
                source.allocateNew(2);
                source.set(0, 0L);
                source.setNull(1);
                input.setRowCount(2);
                long baseline = allocator.getAllocatedMemory();
                try (VectorSchemaRoot output = evaluator.evaluate(input)) {
                    assertEquals(delta, ((BigIntVector) output.getVector(0)).get(0));
                    assertTrue(output.getVector(0).isNull(1));
                    assertEquals(0L, source.get(0));
                    assertEquals(provider.describe(options).outputSchema(), output.getSchema());
                }
                assertEquals(baseline, allocator.getAllocatedMemory());
                if (delta != 0) {
                    source.set(0, delta > 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
                    assertThrows(ArithmeticException.class, () -> evaluator.evaluate(input));
                    assertEquals(baseline, allocator.getAllocatedMemory());
                }
            }
            assertEquals(0L, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies malformed and out-of-range deltas fail on the driver before loading native code.
     *
     * @param value invalid delta option
     */
    @ParameterizedTest
    @ValueSource(strings = {"", "1.5", "invalid", "9223372036854775808", "-9223372036854775809"})
    public void rejectsInvalidDeltaDuringDescription(String value) {
        NativeIncrementProvider provider = new NativeIncrementProvider();
        assertThrows(IllegalArgumentException.class, () -> provider.describe(Map.of("delta", value)));
    }

    /**
     * Verifies evaluator construction reports an unavailable native library as an argument error.
     */
    @Test
    public void rejectsMissingLibrary() {
        try (RootAllocator allocator = new RootAllocator()) {
            assertThrows(IllegalArgumentException.class,
                    () -> new NativeArrowEvaluator(allocator, "/missing/libspyt_native_udf.so", "IncrementInt64", 1L));
        }
    }
}
