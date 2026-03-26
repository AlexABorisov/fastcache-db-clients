package com.fastcache.client;

import com.fastcache.grpc.BinaryPayload;
import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.Value;
import com.google.protobuf.ByteString;

public class KeyUtils {

    /**
     * Creates a Key with a clientId for Global Lock authorization.
     */
    public static Key createKey(String keyStr, int clientId) {
        return Key.newBuilder()
                .setPayload(BinaryPayload.newBuilder()
                                    .setPayload(ByteString.copyFromUtf8(keyStr))
                                    .setSize(keyStr.length())
                                    .build())
                .setClientId(clientId)
                .build();
    }

    /**
     * Creates a Key using a pre-calculated KeyHint (Strong/Week hashes).
     */
    public static Key createKey(String keyStr, KeyHint hint, int clientId) {
        return Key.newBuilder()
                .setPayload(BinaryPayload.newBuilder()
                                    .setPayload(ByteString.copyFromUtf8(keyStr))
                                    .setSize(keyStr.length())
                                    .build())
                .setKeyHint(hint)
                .setClientId(clientId)
                .build();
    }

    /**
     * Helper to wrap raw bytes into a Protobuf Value object.
     */
    public static Value createValue(byte[] data) {
        // We use CompressionUtils here to ensure consistency
        return CompressionUtils.compressIfNeeded(data);
    }
}