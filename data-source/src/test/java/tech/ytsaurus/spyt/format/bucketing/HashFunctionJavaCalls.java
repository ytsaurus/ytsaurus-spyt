package tech.ytsaurus.spyt.format.bucketing;

// Java call sites of the public API, written exactly as a Java caller would: javac resolves the overloads here,
// so these pin the Java contract that a Scala test cannot exercise.
public final class HashFunctionJavaCalls {
    private HashFunctionJavaCalls() {
    }

    public static long nullLiteral() {
        return HashFunction.FARM_HASH.hash(null);
    }

    public static long nullObject() {
        return HashFunction.FARM_HASH.hash((Object) null);
    }

    public static long typedNullBytes() {
        byte[] value = null;
        return HashFunction.FARM_HASH.hash(value);
    }

    public static long typedNullBytesAsObject() {
        byte[] value = null;
        return HashFunction.FARM_HASH.hash((Object) value);
    }

    public static long typedNullBytesAsArgument() {
        byte[] value = null;
        return HashFunction.FARM_HASH.hashArguments(value);
    }

    public static long typedNullString() {
        String value = null;
        return HashFunction.FARM_HASH.hash(value);
    }

    public static long bytes(byte[] value) {
        return HashFunction.FARM_HASH.hash(value);
    }

    public static long primitiveLong(long value) {
        return HashFunction.FARM_HASH.hash(value);
    }

    public static long objectArrayAsSingleArgument(Object[] value) {
        return HashFunction.FARM_HASH.hash(value);
    }

    public static long spread(Object[] arguments) {
        return HashFunction.FARM_HASH.hashArguments(arguments);
    }

    public static long nullArray() {
        return HashFunction.FARM_HASH.hashArguments((Object[]) null);
    }

    public static long singleNullArgument() {
        return HashFunction.FARM_HASH.hashArguments((Object) null);
    }

    public static long noArguments() {
        return HashFunction.FARM_HASH.hashArguments();
    }

    public static long two(long first, String second) {
        return HashFunction.FARM_HASH.hashArguments(first, second);
    }

    public static long callWithNullValues(HashFunctionCall call) {
        return call.hash((Object[]) null);
    }
}
