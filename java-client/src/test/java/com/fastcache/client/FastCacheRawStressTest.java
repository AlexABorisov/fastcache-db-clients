package com.fastcache.client;

import com.fastcache.client.standalone.TestBase;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FastCacheRawStressTest {

    private final int THREAD_COUNT = 32; // Optimized for i9
    private final int OPERATIONS_PER_THREAD = 100000;
    private String serverName;

    @BeforeEach
    void init() throws IOException {
        serverName = "stress-server-" + UUID.randomUUID();
        InProcessServerBuilder.forName(serverName)
                .addService(new TestBase.MockFastCacheService())
                .build()
                .start();
    }

    @Test
    void highConcurrencyLoadTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                // Each thread gets its own Async Client (simulating multiple microservices)
                FastCacheAsyncClient client = new FastCacheAsyncClient(
                        InProcessChannelBuilder.forName(serverName).build());

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "stress:" + threadId + ":" + j;
                        byte[] data = ("value_data_" + j).getBytes(StandardCharsets.UTF_8);

                        // Mix of operations
                        try {
                            // 1. Write
                            client.createKeyValue(key, data).get(5, TimeUnit.SECONDS);

                            // 2. Immediate Read
                            byte[] result = client.getValue(key).get(5, TimeUnit.SECONDS);

                            // 3. Update
                            client.updateKeyValue(key, (new String(data) + "_updated").getBytes())
                                    .get(5, TimeUnit.SECONDS);

                            if (result != null) {
                                successCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    try {
                        client.shutdown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    latch.countDown();
                }
            });
        }

        latch.await(10, TimeUnit.MINUTES);
        long duration = System.currentTimeMillis() - startTime;
        double opsPerSec = (double) (THREAD_COUNT * OPERATIONS_PER_THREAD) / (duration / 1000.0);
        executor.shutdown();

        System.out.println("--- Stress Test Results ---");
        System.out.println("Total Operations: " + (THREAD_COUNT * OPERATIONS_PER_THREAD));
        System.out.println("Successes: " + successCount.get());
        System.out.println("Errors: " + errorCount.get());
        System.out.println("Duration: " + duration + "ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec");

        Assertions.assertEquals(0, errorCount.get(), "Stress test encountered RPC errors!");
    }
}