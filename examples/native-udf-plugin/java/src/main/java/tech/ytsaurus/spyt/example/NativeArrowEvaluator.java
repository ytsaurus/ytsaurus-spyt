package tech.ytsaurus.spyt.example;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import tech.ytsaurus.spyt.format.columnar.ColumnarEvaluator;

/**
 * Imports newly allocated native Arrow results for all of the example's function ABIs.
 *
 * <p>Providers create one instance per task and reuse it for batches. Inputs are borrowed and exported
 * without copying when their vectors share an allocator root; otherwise they are copied before export.
 * Native code consumes only the export references. Returned roots own native buffers
 * through Arrow release callbacks. Close every output before closing this evaluator, because those
 * callbacks reside in its loaded library. The supplied allocator remains owned by the executor context.
 */
final class NativeArrowEvaluator implements ColumnarEvaluator {

    private final Arena arena = Arena.ofShared();
    private final BufferAllocator allocator;
    private final String symbol;
    private final MethodHandle handle;
    private boolean closed;

    /**
     * Binds the four-pointer ABI, optionally binding a scalar between input and output pointers.
     *
     * @param allocator borrowed allocator for exports and imported vectors
     * @param libraryPath resolved executor-local shared-library path
     * @param symbol native symbol implementing the selected ABI
     * @param scalar a {@link Long} increment or {@link Double} scale, or {@code null} for no scalar
     */
    NativeArrowEvaluator(BufferAllocator allocator, String libraryPath, String symbol, Number scalar) {
        this.allocator = allocator;
        this.symbol = symbol;
        try {
            SymbolLookup lookup = SymbolLookup.libraryLookup(libraryPath, arena);
            FunctionDescriptor descriptor = FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
            if (scalar != null) {
                ValueLayout layout = switch (scalar) {
                    case Long value -> ValueLayout.JAVA_LONG;
                    case Double value -> ValueLayout.JAVA_DOUBLE;
                    default -> throw new IllegalArgumentException("Expected a Long or Double native scalar");
                };
                descriptor = descriptor.insertArgumentLayouts(2, layout);
            }
            MethodHandle downcall = Linker.nativeLinker().downcallHandle(lookup.find(symbol).orElseThrow(
                    () -> new IllegalArgumentException(symbol + " not found in " + libraryPath)), descriptor);
            handle = scalar == null ? downcall : MethodHandles.insertArguments(downcall, 2, scalar);
        } catch (RuntimeException | Error error) {
            arena.close();
            throw error;
        }
    }

    /**
     * Exports a borrowed input batch and transfers the native result into a caller-owned Arrow root.
     *
     * @param input record batch matching the provider's declared input schema
     * @return native-allocated result, to close before this evaluator
     * @throws IllegalStateException if closed, native execution fails, or the downcall fails
     * @throws ArithmeticException if native execution reports numeric or output-size overflow
     */
    @Override
    public VectorSchemaRoot evaluate(VectorSchemaRoot input) {
        if (closed) {
            throw new IllegalStateException("Native Arrow evaluator is closed");
        }
        try (ExportedArrowBatch batch = exportInput(input);
             ArrowArray outputArray = ArrowArray.allocateNew(allocator);
             ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator)) {
            try {
                int status = (int) handle.invokeExact(batch.arraySegment(), batch.schemaSegment(),
                        MemorySegment.ofAddress(outputArray.memoryAddress()),
                        MemorySegment.ofAddress(outputSchema.memoryAddress()));
                if (status == -4) {
                    throw new ArithmeticException(symbol + ": " + statusMessage(status));
                }
                if (status != 0) {
                    throw new IllegalStateException(
                            symbol + " returned status " + status + ": " + statusMessage(status));
                }
                return importOutput(outputArray, outputSchema);
            } finally {
                try {
                    outputArray.release();
                } finally {
                    outputSchema.release();
                }
            }
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new IllegalStateException(symbol + " native call failed", error);
        }
    }

    /**
     * Exports input using its buffers' allocator hierarchy, which can differ from the task allocator.
     * Arrow's record-batch exporter reloads buffers into temporary vectors and requires a shared root
     * allocator. Inputs spanning multiple roots are copied into the task allocator before export.
     *
     * @param input borrowed input whose vectors remain unchanged
     * @return scoped export retaining all buffers required by native code
     */
    private ExportedArrowBatch exportInput(VectorSchemaRoot input) {
        BufferAllocator inputAllocator = input.getFieldVectors().isEmpty()
                ? allocator : input.getVector(0).getAllocator();
        boolean sharedRoot = input.getFieldVectors().stream().allMatch(
                vector -> vector.getAllocator().getRoot() == inputAllocator.getRoot());
        if (sharedRoot) {
            return new ExportedArrowBatch(inputAllocator, input);
        }
        try (VectorSchemaRoot copy = VectorSchemaRoot.create(input.getSchema(), allocator)) {
            copy.allocateNew();
            for (int column = 0; column < input.getFieldVectors().size(); column++) {
                for (int row = 0; row < input.getRowCount(); row++) {
                    copy.getVector(column).copyFromSafe(row, row, input.getVector(column));
                }
            }
            copy.setRowCount(input.getRowCount());
            return new ExportedArrowBatch(allocator, copy);
        }
    }

    /**
     * Imports a result while keeping descriptor storage scoped to the surrounding native call.
     *
     * @param array output array whose ownership is transferred under the C Data protocol
     * @param schema output schema consumed during import
     * @return caller-owned root, closed here if importing the buffers fails
     */
    private VectorSchemaRoot importOutput(ArrowArray array, ArrowSchema schema) {
        VectorSchemaRoot output = VectorSchemaRoot.create(Data.importSchema(allocator, schema, null, false), allocator);
        try {
            Data.importIntoVectorSchemaRoot(allocator, array, output, null, false);
            return output;
        } catch (RuntimeException | Error error) {
            output.close();
            throw error;
        }
    }

    /**
     * Describes status codes shared by the bundled C++ functions.
     *
     * @param status nonzero native status
     * @return stable diagnostic text without requiring an additional native call
     */
    private static String statusMessage(int status) {
        return switch (status) {
            case -1 -> "Arrow input import failed";
            case -2 -> "unexpected input types or column count";
            case -3 -> "native computation or output export failed";
            case -4 -> "numeric or output size overflow";
            default -> "unknown native error";
        };
    }

    /** Releases the loaded library after all returned roots have been closed; repeated calls are harmless. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            arena.close();
        }
    }
}
