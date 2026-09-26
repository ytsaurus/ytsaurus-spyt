package tech.ytsaurus.spyt.format.columnar;

import java.util.Objects;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Describes a columnar SQL function's input schema, output schema, and determinism.
 *
 * <p>Return a descriptor from {@link ColumnarFunctionProvider#describe}. Input fields correspond
 * to SQL arguments by position. The output schema must contain one field; use a struct field for
 * several result values. The descriptor declares decoded Arrow data, not dictionary encodings.
 * SPYT uses it for analysis and validates executor descriptions and output batches against it.
 */
public final class ColumnarFunctionDescriptor {

    private final Schema inputSchema;
    private final Schema outputSchema;
    private final boolean deterministic;

    /**
     * Creates the metadata for a row-preserving function.
     *
     * @param inputSchema positional input fields, including types and nullability
     * @param outputSchema the single result field, including its name, type, and nullability
     * @param deterministic whether equal inputs and options always produce equal results
     * @throws NullPointerException if either schema is null
     * @throws IllegalArgumentException if output has other than one field or any field is dictionary encoded
     */
    public ColumnarFunctionDescriptor(Schema inputSchema, Schema outputSchema, boolean deterministic) {
        this.inputSchema = Objects.requireNonNull(inputSchema, "inputSchema");
        this.outputSchema = Objects.requireNonNull(outputSchema, "outputSchema");
        this.deterministic = deterministic;
        if (outputSchema.getFields().size() != 1) {
            throw new IllegalArgumentException(
                    "A columnar SQL function must return exactly one field (which may be a struct)");
        }
        inputSchema.getFields().forEach(ColumnarFunctionDescriptor::validateField);
        outputSchema.getFields().forEach(ColumnarFunctionDescriptor::validateField);
    }

    /**
     * Rejects dictionary encodings recursively so evaluators receive decoded vectors.
     *
     * @param field top-level or nested field to check during descriptor construction
     * @throws IllegalArgumentException if this field or a descendant declares a dictionary
     */
    private static void validateField(Field field) {
        if (field.getDictionary() != null) {
            throw new IllegalArgumentException("Columnar function schemas must use decoded Arrow fields");
        }
        field.getChildren().forEach(ColumnarFunctionDescriptor::validateField);
    }

    /**
     * Returns the schema used to bind SQL arguments and construct each evaluator input view.
     *
     * @return the positional input schema supplied at construction
     */
    public Schema inputSchema() {
        return inputSchema;
    }

    /**
     * Returns the exact schema that each result batch must expose.
     *
     * @return the one-field output schema, which may contain a struct
     */
    public Schema outputSchema() {
        return outputSchema;
    }

    /**
     * Supplies the determinism declaration used by Spark's expression optimizer.
     *
     * @return true when repeated evaluation of equal inputs with equal options has equal results
     */
    public boolean deterministic() {
        return deterministic;
    }
}
