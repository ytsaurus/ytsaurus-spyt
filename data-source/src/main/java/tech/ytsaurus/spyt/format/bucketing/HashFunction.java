package tech.ytsaurus.spyt.format.bucketing;

import org.apache.spark.unsafe.types.UTF8String;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * A YTsaurus hash function of computed column expressions, reproduced on the JVM for the scalar types bucketing
 * columns have. Per yt/yt/client/table_client/unversioned_value.cpp and library/cpp/yt/farmhash/farm_hash.h each
 * argument is fingerprinted by its value type &mdash; int64 as its two's-complement bits reinterpreted as uint64
 * (never serialized, so no byte order is involved), uint64 as is, boolean as 0/1, null as 0, string as its raw bytes
 * (a {@link String} is UTF-8 encoded first, a Catalyst {@link UTF8String} contributes its bytes unchanged) &mdash;
 * and the fingerprints are folded in argument order, so a multi-argument hash is not a hash of concatenated bytes.
 * Limits: double (rejected by the YT query engine too), any/composite and every other type are refused, not guessed.
 */
public enum HashFunction {
    FARM_HASH("farm_hash") {
        @Override
        protected long fingerprint(long value) {
            return FarmHash.fingerprint(value);
        }

        @Override
        protected long fingerprint(byte[] value) {
            return FarmHash.fingerprintBytes(value);
        }

        @Override
        protected long combine(long[] fingerprints) {
            return FarmHash.fold(fingerprints);
        }
    };

    private final String ytName;

    HashFunction(String ytName) {
        this.ytName = ytName;
    }

    public String ytName() {
        return ytName;
    }

    /** Hashes a single argument; {@code null} is the YTsaurus null value. */
    public long hash(Object argument) {
        return combine(new long[]{fingerprint(argument)});
    }

    /** Hashes an argument list in order; a null array is a caller bug, pass {@code (Object) null} for a null argument. */
    public long hashArguments(Object... arguments) {
        Objects.requireNonNull(arguments, "arguments array is null; pass (Object) null to hash a single null argument");
        long[] fingerprints = new long[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            fingerprints[i] = fingerprint(arguments[i]);
        }
        return combine(fingerprints);
    }

    protected abstract long fingerprint(long value);

    protected abstract long fingerprint(byte[] value);

    protected abstract long combine(long[] fingerprints);

    private long fingerprint(Object argument) {
        if (argument == null) {
            return fingerprint(0L);
        }
        if (argument instanceof Long || argument instanceof Integer
                || argument instanceof Short || argument instanceof Byte) {
            return fingerprint(((Number) argument).longValue());
        }
        if (argument instanceof Boolean) {
            return fingerprint((Boolean) argument ? 1L : 0L);
        }
        if (argument instanceof byte[]) {
            return fingerprint((byte[]) argument);
        }
        if (argument instanceof String) {
            return fingerprint(((String) argument).getBytes(StandardCharsets.UTF_8));
        }
        if (argument instanceof UTF8String) {
            return fingerprint(((UTF8String) argument).getBytes());
        }
        String hint = argument instanceof Object[] ? "; use hashArguments for an argument list" : "";
        throw new IllegalArgumentException(ytName + " accepts int64, uint64, boolean, string and null arguments, got "
                + argument.getClass().getName() + hint);
    }

    public static Optional<HashFunction> byYtName(String name) {
        if (name == null) {
            return Optional.empty();
        }
        String normalized = name.toLowerCase(Locale.ROOT);
        return Arrays.stream(values()).filter(function -> function.ytName.equals(normalized)).findFirst();
    }
}
