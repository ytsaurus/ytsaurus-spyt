package tech.ytsaurus.spyt.format.bucketing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class HashFunctionCall implements Serializable {
    private static final long serialVersionUID = 1L;

    // a real bucket count is always positive, so 0 marks a call without "% buckets"
    private static final int NO_BUCKETS = 0;

    private final HashFunction function;

    private final List<String> arguments;

    private final int buckets;

    public HashFunctionCall(HashFunction function, List<String> arguments) {
        this.function = Objects.requireNonNull(function, "function");
        this.arguments = columns(arguments);
        this.buckets = NO_BUCKETS;
    }

    public HashFunctionCall(HashFunction function, List<String> arguments, int buckets) {
        this.function = Objects.requireNonNull(function, "function");
        this.arguments = columns(arguments);
        if (buckets <= 0) {
            throw new IllegalArgumentException("bucket count must be positive, got " + buckets);
        }
        this.buckets = buckets;
    }

    private static List<String> columns(List<String> arguments) {
        Objects.requireNonNull(arguments, "arguments");
        if (arguments.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("column names must not be null, got " + arguments);
        }
        return Collections.unmodifiableList(new ArrayList<>(arguments));
    }

    public HashFunction function() {
        return function;
    }

    public List<String> arguments() {
        return arguments;
    }

    public Optional<Integer> buckets() {
        return buckets == NO_BUCKETS ? Optional.empty() : Optional.of(buckets);
    }

    /** Hashes one value per column, in column order; a null array is a caller bug, pass {@code (Object) null} for a null value. */
    public long hash(Object... argumentValues) {
        Objects.requireNonNull(argumentValues, "argument values array is null; pass (Object) null for a null value");
        if (argumentValues.length != arguments.size()) {
            throw new IllegalArgumentException(this + " takes " + arguments.size() + " values, got "
                    + argumentValues.length);
        }
        return function.hashArguments(argumentValues);
    }

    public long bucket(Object... argumentValues) {
        if (buckets == NO_BUCKETS) {
            throw new IllegalStateException(this + " has no bucket count");
        }
        return Long.remainderUnsigned(hash(argumentValues), buckets);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof HashFunctionCall)) {
            return false;
        }
        HashFunctionCall that = (HashFunctionCall) other;
        return function == that.function
                && buckets == that.buckets
                && arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, arguments, buckets);
    }

    @Override
    public String toString() {
        return "HashFunctionCall(" + function + ", " + arguments + ", " + buckets() + ")";
    }
}
