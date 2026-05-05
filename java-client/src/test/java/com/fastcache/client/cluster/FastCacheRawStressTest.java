package com.fastcache.client.cluster;

import com.fastcache.TestBase;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
        InProcessServerBuilder.forName(serverName).addService(new TestBase.MockFastCacheService()).build().start();
    }

    @Test
    void highConcurrencyCreateLoadTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch write = new CountDownLatch(THREAD_COUNT);

        AtomicInteger writeSuccessCount = new AtomicInteger(0);
        AtomicInteger writeErrorCount = new AtomicInteger(0);

        FastCacheAsyncSmartClient client = new FastCacheAsyncSmartClient("127.0.0.1",
                                                                         51000,0,
                                                                         Duration.ofSeconds(1));
        while (!client.getReadyFlag()) {
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ConcurrentHashMap<String,KeyHint> storage = new ConcurrentHashMap<>();
        System.out.println("Write start");
        long readStartTime = System.currentTimeMillis();
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                // Each thread gets its own Async Client (simulating multiple microservices)

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "stress:" + threadId + ":" + j;
                        byte[] data = ("value_data_" + j).getBytes(StandardCharsets.UTF_8);
                        KeyHint keyHint = null;
                        // Mix of operations
                        try {
                            // 1. Write
                            keyHint = client.createKeyValue(key, data).get(500, TimeUnit.MILLISECONDS);
                            storage.put(key,keyHint);
                            writeSuccessCount.incrementAndGet();
                        } catch (Throwable e) {
                            writeErrorCount.incrementAndGet();
                            if (keyHint != null) System.err.println("Hint" + keyHint);
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
                finally {
                    write.countDown();
                }
            });
        }

        write.await(60, TimeUnit.MINUTES);
        long writeDuration = System.currentTimeMillis() - readStartTime;
        double opsPerSec = (double) (THREAD_COUNT * OPERATIONS_PER_THREAD) / (writeDuration / 1000.0);

        System.out.println("--- Stress Test Write Results ---");
        System.out.println("Total Operations: " + (THREAD_COUNT * OPERATIONS_PER_THREAD));
        System.out.println("Successes: " + writeSuccessCount.get());
        System.out.println("Errors: " + writeErrorCount.get());
        System.out.println("Duration: " + writeDuration + "ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec");

        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        CountDownLatch readLatch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readErrorCount = new AtomicInteger(0);

        while (!client.getReadyFlag()) {
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Real start read");
        readStartTime = System.currentTimeMillis();
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                // Each thread gets its own Async Client (simulating multiple microservices)

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "stress:" + threadId + ":" + j;
                        byte[] data = ("value_data_" + j).getBytes(StandardCharsets.UTF_8);

                        // Mix of operations
                        KeyHint hint=null;
                        try {

                            // 2. Immediate Read
                            hint = storage.get(key);
                            byte[] result = client.getValue(key, hint).get(500, TimeUnit.MILLISECONDS);


                            if (result != null) {
                                readSuccessCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            readErrorCount.incrementAndGet();
                            System.err.println("Hint "+hint);
                        }
                    }
                } finally {
                    readLatch.countDown();
                }
            });
        }

        readLatch.await(60, TimeUnit.MINUTES);
        long readDuration = System.currentTimeMillis() - readStartTime;
        opsPerSec = (double) (THREAD_COUNT * OPERATIONS_PER_THREAD) / (readDuration / 1000.0);

        System.out.println("--- Stress Test Read Results ---");
        System.out.println("Total Operations: " + (THREAD_COUNT * OPERATIONS_PER_THREAD));
        System.out.println("Successes: " + readSuccessCount.get());
        System.out.println("Errors: " + readErrorCount.get());
        System.out.println("Duration: " + readDuration + "ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec");

        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        CountDownLatch updateLatch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger updateSuccessCount = new AtomicInteger(0);
        AtomicInteger updateErrorCount = new AtomicInteger(0);


        while (!client.getReadyFlag()) {
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Real start update");
        readStartTime = System.currentTimeMillis();
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                // Each thread gets its own Async Client (simulating multiple microservices)

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "stress:" + threadId + ":" + j;
                        byte[] data = ("value_data_" + j).getBytes(StandardCharsets.UTF_8);

                        // Mix of operations
                        try {

                            KeyHint hint = storage.get(key);

                            byte[] result = client.updateKeyValue(key, hint, (new String(data) + "_updated").getBytes())
                                    .get(500, TimeUnit.MILLISECONDS);
                            //client.remove(key).get();

                            if (result != null) {
                                updateSuccessCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            updateErrorCount.incrementAndGet();
                            System.err.println(e.getCause().getLocalizedMessage());
                        }
                    }
                } finally {
                    updateLatch.countDown();
                }
            });
        }

        updateLatch.await(60, TimeUnit.MINUTES);
        client.shutdown();
        long updateDuration = System.currentTimeMillis() - readStartTime;
        opsPerSec = (double) (THREAD_COUNT * OPERATIONS_PER_THREAD) / (updateDuration / 1000.0);
        executor.shutdown();

        System.out.println("--- Stress Test Update Results ---");
        System.out.println("Total Operations: " + (THREAD_COUNT * OPERATIONS_PER_THREAD));
        System.out.println("Successes: " + updateSuccessCount.get());
        System.out.println("Errors: " + updateErrorCount.get());
        System.out.println("Duration: " + updateDuration + "ms");
        System.out.println("Throughput: " + String.format("%.2f", opsPerSec) + " ops/sec");

    }

    @Test
    void highConcurrencyReadLoadTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        FastCacheAsyncSmartClient client = new FastCacheAsyncSmartClient("127.0.0.1",
                                                                         61000,0,
                                                                         Duration.ofSeconds(1));
        while (!client.getReadyFlag()) {
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Real start");
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                // Each thread gets its own Async Client (simulating multiple microservices)

                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "stress:" + threadId + ":" + j;
                        byte[] data = ("value_data_" + j).getBytes(StandardCharsets.UTF_8);

                        // Mix of operations
                        try {
                            // 1. Write
                            KeyHint keyHint = client.createKeyValue(key, data).get(100, TimeUnit.MILLISECONDS);

                            // 2. Immediate Read
                            byte[] result = client.getValue(key,keyHint).get(100, TimeUnit.MILLISECONDS);

                            // 3. Update
                            client.updateKeyValue(key,keyHint, (new String(data) + "_updated").getBytes())
                                    .get(100, TimeUnit.MILLISECONDS);
                            //client.remove(key).get();

                            if (result != null) {
                                successCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    client.shutdown();
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.MINUTES);
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