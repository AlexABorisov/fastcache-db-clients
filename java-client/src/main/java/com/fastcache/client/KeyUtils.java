package com.fastcache.client;

import com.fastcache.grpc.BinaryPayload;
import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.Value;
import com.fastcache.utils.CompressionUtils;
import com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;

public class KeyUtils {

    /**
     * Creates a Key with a clientId for Global Lock authorization.
     */
    public static Key createKey(String keyStr, int clientId) {
        return CompressionUtils.compressKeyIfNeeded(keyStr.getBytes(StandardCharsets.UTF_8),clientId).build();
    }

    /**
     * Creates a Key using a pre-calculated KeyHint (Strong/Week hashes).
     */
    public static Key createKey(String keyStr, KeyHint hint, int clientId) {
        return CompressionUtils.compressKeyIfNeeded(keyStr.getBytes(StandardCharsets.UTF_8),clientId).setKeyHint(hint).build();
    }

    /**
     * Helper to wrap raw bytes into a Protobuf Value object.
     */
    public static Value createValue(byte[] data) {
        // We use CompressionUtils here to ensure consistency
        return CompressionUtils.compressIfNeeded(data);
    }
}