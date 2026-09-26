package tech.ytsaurus.spyt.example;

import java.lang.foreign.MemorySegment;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Scoped export of an Arrow Java record batch through the Arrow C Data Interface.
 *
 * <p>Use this Java 25 helper in a companion JAR when invoking a native function that accepts
 * {@code ArrowArray*} and {@code ArrowSchema*}. Construct it in a try-with-resources block around
 * the native call, and supply its addresses or FFM segments according to the library's ABI.
 * This helper does not choose native symbols, signatures, or status/error conventions.
 *
 * <p>The export retains Arrow references without transferring ownership of the Java batch.
 * Closing it releases remaining C Data exports and descriptor storage, not the original root
 * or allocator. A native importer may consume the exports under the C Data ownership protocol;
 * it must then release the imported data itself. Never keep the descriptor addresses beyond the
 * export's lifetime. Exporting a borrowed input does not permit native code to mutate it: copy
 * data first when adapting an in-place native function.
 */
public final class ExportedArrowBatch implements AutoCloseable {

    private ArrowArray array;
    private ArrowSchema schema;
    private boolean closed;

    /**
     * Allocates C descriptors and exports the supplied batch, retaining its buffers as needed.
     *
     * <p>Use a live allocator that remains open until this export is closed. No dictionary
     * provider is supplied, so the batch must use decoded vectors. Partially created exports
     * are released if construction fails.
     * Arrow's exporter reloads buffers into temporary vectors, so this allocator must share a
     * root with the batch's buffers. For borrowed scan input, use its vector allocator rather
     * than assuming the evaluator's output allocator shares that root. Copy batches spanning
     * multiple roots into a common allocator before exporting them.
     *
     * @param allocator borrowed allocator for C descriptor storage and export bookkeeping
     * @param batch borrowed Arrow root to expose to native code
     */
    public ExportedArrowBatch(BufferAllocator allocator, VectorSchemaRoot batch) {
        try {
            array = ArrowArray.allocateNew(allocator);
            schema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, batch, null, array, schema);
        } catch (RuntimeException | Error error) {
            try {
                close();
            } catch (RuntimeException | Error cleanup) {
                error.addSuppressed(cleanup);
            }
            throw error;
        }
    }

    /**
     * Supplies the address of the exported record batch's C {@code ArrowArray} descriptor.
     *
     * @return descriptor address, valid only while this export remains open
     * @throws IllegalStateException if the export has been closed
     */
    public long arrayAddress() {
        ensureOpen();
        return array.memoryAddress();
    }

    /**
     * Supplies the address of the C {@code ArrowSchema} describing the exported record batch.
     *
     * @return descriptor address, valid only while this export remains open
     * @throws IllegalStateException if the export has been closed
     */
    public long schemaAddress() {
        ensureOpen();
        return schema.memoryAddress();
    }

    /**
     * Wraps the array descriptor address for an FFM downcall parameter of type {@code ADDRESS}.
     *
     * <p>This is an address-only segment, not a Java view of the descriptor's fields. It does not
     * extend the export's lifetime; do not dereference or pass it after closing the export.
     *
     * @return address segment for the exported {@code ArrowArray}
     * @throws IllegalStateException if the export has been closed
     */
    public MemorySegment arraySegment() {
        return MemorySegment.ofAddress(arrayAddress());
    }

    /**
     * Wraps the schema descriptor address for an FFM downcall parameter of type {@code ADDRESS}.
     *
     * <p>The returned address-only segment does not own storage or extend this export's lifetime.
     *
     * @return address segment for the exported {@code ArrowSchema}
     * @throws IllegalStateException if the export has been closed
     */
    public MemorySegment schemaSegment() {
        return MemorySegment.ofAddress(schemaAddress());
    }

    /**
     * Guards descriptor access so callers cannot obtain addresses after storage has been freed.
     *
     * @throws IllegalStateException if cleanup has already started
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Arrow export is closed");
        }
    }

    /**
     * Releases unconsumed exports and frees both C descriptor structures.
     *
     * <p>Call after the native call has finished using these descriptor addresses. Cleanup
     * attempts both descriptors even if releasing one fails. Repeated calls have no effect;
     * the original Java batch and the borrowed allocator remain owned by their callers.
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (array != null) {
                    try {
                        array.release();
                    } finally {
                        array.close();
                    }
                }
            } finally {
                if (schema != null) {
                    try {
                        schema.release();
                    } finally {
                        schema.close();
                    }
                }
            }
        }
    }
}
