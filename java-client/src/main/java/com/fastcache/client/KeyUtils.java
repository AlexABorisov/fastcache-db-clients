package com.fastcache.client;

import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.Value;
import com.fastcache.utils.CompressionUtils;

import java.nio.charset.StandardCharsets;

public class KeyUtils {

    /**
     * Creates a Key with a clientId for Global Lock authorization.
     */
    public static Key createKey(String keyStr, int clientId) {
        return CompressionUtils.compressKeyIfNeeded(keyStr.getBytes(StandardCharsets.UTF_8), clientId).build();
    }

    public static Key createKey(byte[] keyStr, int clientId) {
        KeyHint keyHint = KeyHint.newBuilder().setWeekHash(weekHash(keyStr, keyStr.length, 0)).build();
        return CompressionUtils.compressKeyIfNeeded(keyStr, clientId).setKeyHint(keyHint).build();
    }

    /**
     * Creates a Key using a pre-calculated KeyHint (Strong/Week hashes).
     */
    public static Key createKey(String keyStr, KeyHint hint, int clientId) {
        return CompressionUtils.compressKeyIfNeeded(keyStr.getBytes(StandardCharsets.UTF_8), clientId)
                .setKeyHint(hint)
                .build();
    }

    public static Key createKey(byte[] keyStr, KeyHint hint, int clientId) {
        if (hint != null) {
            return CompressionUtils.compressKeyIfNeeded(keyStr, clientId).setKeyHint(hint).build();
        }
        KeyHint keyHint = KeyHint.newBuilder().setWeekHash(weekHash(keyStr, keyStr.length, 0)).build();
        return CompressionUtils.compressKeyIfNeeded(keyStr, clientId).setKeyHint(keyHint).build();
    }

    /**
     * Helper to wrap raw bytes into a Protobuf Value object.
     */
    public static Value createValue(byte[] data) {
        // We use CompressionUtils here to ensure consistency
        return CompressionUtils.compressIfNeeded(data);
    }
    private static final long PRIME_0 = 0x9E3779B97F4A7C15L;
    private static final long PRIME_1 = 0xBF58476D1CE4E5B9L;

    public static int weekHash(byte[] data, int len, long seed) {
        long a = seed ^ PRIME_0;
        long b = len ^ PRIME_1;
        int offset = 0;

        // Process 64-bit blocks
        while (len >= 8) {
            long val = readLongLE(data, offset);

            // Simulating mux64 (Multiply and get high/low 64 bits)
            long[] result = mux64(val ^ a, PRIME_0);
            long low = result[0];
            long high = result[1];

            a ^= high;
            b += low;

            offset += 8;
            len -= 8;
        }

        // Handle tails
        if (len > 4) {
            int tail = readIntLE(data, offset);
            b ^= (tail & 0xFFFFFFFFL); // Treat as unsigned
            offset += 4;
            len -= 4;
        }

        if (len > 2) {
            int tail = readShortLE(data, offset);
            b ^= (tail & 0xFFFFL);
            offset += 2;
            len -= 2;
        }

        if (len >= 1) {
            int tail = data[offset] & 0xFF;
            b ^= tail;
        }

        // Final mix
        long[] finalResult = mux64(a ^ Long.rotateLeft(b, 17), PRIME_1);
        long finalLow = finalResult[0];
        long finalHigh = finalResult[1];

        return (int) (finalLow ^ finalHigh);
    }

    /**
     * Simulates the mux64 (multiplication producing 128-bit result)
     */
    private static long[] mux64(long a, long b) {
        // In Java 9+, we can use Math.multiplyHigh for the high 64 bits.
        long low = a * b;
        long high = Math.multiplyHigh(a, b);
        return new long[]{low, high};
    }

    // Helper methods to read Little-Endian values from byte array
    private static long readLongLE(byte[] data, int offset) {
        return ((long) (data[offset] & 0xFF)) |
               ((long) (data[offset + 1] & 0xFF) << 8) |
               ((long) (data[offset + 2] & 0xFF) << 16) |
               ((long) (data[offset + 3] & 0xFF) << 24) |
               ((long) (data[offset + 4] & 0xFF) << 32) |
               ((long) (data[offset + 5] & 0xFF) << 40) |
               ((long) (data[offset + 6] & 0xFF) << 48) |
               ((long) (data[offset + 7] & 0xFF) << 56);
    }

    private static int readIntLE(byte[] data, int offset) {
        return (data[offset] & 0xFF) |
               ((data[offset + 1] & 0xFF) << 8) |
               ((data[offset + 2] & 0xFF) << 16) |
               ((data[offset + 3] & 0xFF) << 24);
    }

    private static int readShortLE(byte[] data, int offset) {
        return (data[offset] & 0xFF) |
               ((data[offset + 1] & 0xFF) << 8);
    }
}