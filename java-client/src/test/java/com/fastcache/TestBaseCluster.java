package com.fastcache;

import com.fastcache.client.FastCacheAsyncSmartClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

/**
 * Base class for in-memory gRPC tests.
 * Provides a mock FastCache server implementation for testing the client without a physical server.
 */
public abstract class TestBaseCluster {

    protected FastCacheAsyncSmartClient client;

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    protected byte[] createLargePayload(int size) {
        byte[] bytes = UUID.randomUUID().toString().getBytes();
        byte[] payload = new byte[size];
        System.arraycopy(bytes,0,payload,0 ,bytes.length);
        for (int i = bytes.length; i < size; i++) {
            payload[i] = (byte) (i % 128);
        }
        return payload;
    }

    @BeforeEach
    void setUp() throws IOException {
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000,0, Duration.ofSeconds(3600));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            client.shutdown();
        }
    }

}