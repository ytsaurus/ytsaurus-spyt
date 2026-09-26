package tech.ytsaurus.spyt.format.columnar;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Executes a registered function on successive Arrow batches in one Spark partition.
 *
 * <p>Implementations may use Java computation or a native ABI. Create instances through
 * {@link ColumnarFunctionProvider#create}; SPYT owns their lifecycle and the returned batches.
 * An evaluator need not support concurrent calls, but must support repeated evaluation until
 * closed. Native handles belong to this lifecycle rather than a process-wide session cache.
 */
public interface ColumnarEvaluator extends AutoCloseable {

    /**
     * Computes a distinct output batch with the registered schema and the same number of rows.
     *
     * <p>The input is borrowed and read-only: do not close or mutate its root, vectors, or buffers.
     * SPYT releases its input view after this call. Return an independently owned root; if its
     * vectors share input buffers, retain those buffers independently before returning. Arrow
     * transfers of retained buffers must use allocators sharing the source allocator's root.
     * Allocate fresh output data with the context allocator when copying or modifying values.
     *
     * <p>Ownership of the returned batch transfers to SPYT. Do not reuse or close it after return.
     * If evaluation fails, release partially allocated output and other temporary resources
     * before throwing; SPYT can only release outputs that were successfully returned.
     *
     * @param input borrowed batch matching the descriptor's positional input schema
     * @return non-null, owned batch containing exactly the descriptor's one output field
     */
    VectorSchemaRoot evaluate(VectorSchemaRoot input);

    /**
     * Releases evaluator-owned resources, such as native contexts and library arenas.
     *
     * <p>SPYT calls this after output batches have been released, on task completion,
     * failure, or early termination. Do not close the context allocator: SPYT closes it after
     * evaluator cleanup. Release any child allocators created by the implementation itself.
     */
    @Override
    void close();
}
