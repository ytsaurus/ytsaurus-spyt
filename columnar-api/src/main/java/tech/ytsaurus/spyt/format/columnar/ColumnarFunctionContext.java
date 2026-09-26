package tech.ytsaurus.spyt.format.columnar;

import org.apache.arrow.memory.BufferAllocator;

/**
 * Executor services supplied when SPYT creates a partition's columnar evaluator.
 *
 * <p>Use this context instead of embedding driver filesystem paths or constructing an unmanaged
 * output allocator. It is valid only for the evaluator's partition and must not be cached across
 * tasks or Spark Connect sessions.
 */
public interface ColumnarFunctionContext {

    /**
     * Supplies the allocator for newly allocated output vectors and temporary Arrow data.
     *
     * <p>The allocator belongs to SPYT and is closed after output and evaluator cleanup.
     * Implementations must not close it themselves. Retaining existing input buffers may require
     * the input vector's allocator instead, because Arrow cannot transfer between unrelated roots.
     *
     * @return the partition's SPYT-owned allocator
     */
    BufferAllocator allocator();

    /**
     * Resolves a distributed filename to an absolute path on the executing machine.
     *
     * <p>Distribute files before the query using Spark's {@code --files},
     * {@code SparkContext.addFile}, or Connect's {@code spark.addArtifact(path, file=True)}.
     * Session-scoped files take precedence over shared files with the same name. Pass the
     * returned path to native library loading or ordinary file I/O inside the evaluator.
     *
     * @param filename nonempty basename, without directory components or traversal segments
     * @return the executor-local path of an existing regular file
     * @throws IllegalArgumentException if the name is invalid or the file is unavailable
     */
    String resolveArtifact(String filename);
}
