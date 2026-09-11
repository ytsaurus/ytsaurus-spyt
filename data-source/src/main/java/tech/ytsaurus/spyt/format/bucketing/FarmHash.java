package tech.ytsaurus.spyt.format.bucketing;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public final class FarmHash {
    private static final long MULTIPLIER = 0x9ddfea08eb382d69L;

    private static final long SEED = 0xdeadc0deL;

    private FarmHash() {
    }

    public static long fingerprint(long value) {
        long mixed = value * MULTIPLIER;
        long shifted = (mixed ^ (mixed >>> 44)) * MULTIPLIER;
        return (shifted ^ (shifted >>> 41)) * MULTIPLIER;
    }

    public static long fingerprintBytes(byte[] bytes) {
        return Hashing.farmHashFingerprint64().hashBytes(bytes).asLong();
    }

    public static long combine(long low, long high) {
        long mixed = (low ^ high) * MULTIPLIER;
        return fingerprint(high ^ (mixed ^ (mixed >>> 47)));
    }

    public static long fold(long... fingerprints) {
        long result = SEED;
        for (long fingerprint : fingerprints) {
            result = combine(result, fingerprint);
        }
        return result ^ fingerprints.length;
    }

    public static long hashLong(long value) {
        return fold(fingerprint(value));
    }

    public static long hashBytes(byte[] bytes) {
        return fold(fingerprintBytes(bytes));
    }

    public static long hashString(String value) {
        return hashBytes(value.getBytes(StandardCharsets.UTF_8));
    }
}
