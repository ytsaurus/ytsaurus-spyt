package tech.ytsaurus.spyt.format.columnar;

import java.util.Map;

/**
 * Entry point implemented by a companion JAR to expose a row-preserving Arrow function to SPYT.
 *
 * <p>Provide a public no-argument constructor and register the implementation's class name with
 * {@code spyt.register_columnar_function} or {@code REGISTER COLUMNAR FUNCTION}. SPYT loads the
 * class through the session's artifact classloader, obtains its descriptor on the driver, and
 * creates an evaluator on each executing partition. Evaluators may compute entirely in Java,
 * including with the JDK Vector API, or invoke native code. Native libraries and their ABI are the
 * implementation's responsibility; they are not part of this interface.
 *
 * <p>Keep construction and description free of native initialization. Resolve distributed files
 * and open native resources in {@link #create}, since driver-local paths are not executor paths.
 * This API uses the Arrow Java types supplied by Spark; do not bundle another copy in the plugin.
 */
public interface ColumnarFunctionProvider {

    /** Version of the Java plugin contract understood by this API. */
    int API_VERSION = 1;

    /**
     * Identifies the plugin contract implemented by this provider.
     *
     * @return the supported API version; SPYT rejects a different version before evaluation
     */
    default int apiVersion() {
        return API_VERSION;
    }

    /**
     * Describes the positional inputs and single output field used for SQL planning.
     *
     * <p>SPYT calls this during registration and again on executors. For the same options, both
     * calls must return equal schemas and the same determinism flag. This method must not depend
     * on executor-local artifacts or initialize native resources.
     *
     * @param options immutable string options supplied at registration
     * @return a non-null descriptor with decoded Arrow schemas
     */
    ColumnarFunctionDescriptor describe(Map<String, String> options);

    /**
     * Opens the evaluator used for successive batches in one executor partition.
     *
     * <p>Use the context to resolve library filenames and allocate output vectors. Release any
     * partially initialized resources if construction fails. SPYT closes a successfully returned
     * evaluator after releasing its output batches, including on failure or early termination.
     *
     * @param context partition-owned allocation and artifact services
     * @param options immutable options identical to those used for description
     * @return a non-null evaluator whose state is private to this partition
     */
    ColumnarEvaluator create(ColumnarFunctionContext context, Map<String, String> options);
}
