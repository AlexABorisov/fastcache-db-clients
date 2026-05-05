package com.fastcache.client.standalone;

import com.fastcache.TestBase;
import com.fastcache.client.FastCacheAsyncSimpleClient;
import com.fastcache.grpc.LockType;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ContainerStressTest {

    private final int THREAD_COUNT = 32; // Matching i9 logical cores
    private final int OPS_PER_THREAD = 5000;
    private final String QUEUE_KEY = "stress_queue_01";
    private final String LIST_KEY = "stress_list_01";
    private FastCacheAsyncSimpleClient client;

    @BeforeEach
    void init() throws IOException {
        String serverName = "stress-server-" + UUID.randomUUID();
        var server = InProcessServerBuilder.forName(serverName)
                .addService(new TestBase.MockFastCacheService())
                .build()
                .start();
        client = new FastCacheAsyncSimpleClient(InProcessChannelBuilder.forName(serverName).build(), 999);
    }

    @AfterEach
    void stop() throws InterruptedException {
        client.shutdown();
    }

    /**
     * STRESS: Producer-Consumer on a Single Queue
     * Tests: Shard mutex contention and memory safety of the HugePage-backed queue.
     */
    @Test
    void testConcurrentQueuePushPop() throws Exception {
        client.createQueue(QUEUE_KEY, null).get();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPS_PER_THREAD; j++) {
                        // Alternating PUSH and POP
                        if (j % 2 == 0) {
                            byte[] data = ("val-" + threadId + "-" + j).getBytes();
                            client.addElementToTail(QUEUE_KEY, List.of(data)).get();
                        } else {
                            client.getAndRemoveFront(QUEUE_KEY).get();
                        }
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();

        System.out.printf("Queue Stress Finished: %d ops in %d ms (Avg: %.2f ops/sec)%n",
                          successCount.get(),
                          (end - start),
                          (successCount.get() / ((end - start) / 1000.0)));

        executor.shutdown();
    }

    /**
     * STRESS: Distributed Vector Operations
     * Tests: Sharding efficiency across the 32 shards of the i9 server.
     */
    @Test
    void testShardedVectorThroughput() throws Exception {
        int totalKeys = 1000;
        // Pre-create 1000 vectors to distribute across shards
        for (int i = 0; i < totalKeys; i++) {
            client.createVector("vec_" + i, List.of("init".getBytes()));
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<CompletableFuture<?>> futures = new ArrayList<>();

        long start = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT * OPS_PER_THREAD; i++) {
            String key = "vec_" + (ThreadLocalRandom.current().nextInt(totalKeys));
            byte[] payload = new byte[128]; // 128 byte entries
            ThreadLocalRandom.current().nextBytes(payload);

            // Fire and forget (Async) to maximize gRPC pipeline saturation
            futures.add(client.addElementToTail(key, List.of(payload)));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();

        System.out.printf("Vector Shard Stress: %d appends in %d ms (Avg: %.2f ops/sec)%n",
                          futures.size(),
                          (end - start),
                          (futures.size() / ((end - start) / 1000.0)));

        executor.shutdown();
    }

    @Test
    void testConcurrentListPushPop() throws Exception {
        client.createList(LIST_KEY, null).get();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPS_PER_THREAD; j++) {
                        // Alternating PUSH and POP
                        if (j % 2 == 0) {
                            byte[] data = ("val-" + threadId + "-" + j).getBytes();
                            client.addElementToTail(LIST_KEY, List.of(data)).get();
                        } else {
                            client.getAndRemoveFront(LIST_KEY).get();
                        }
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();

        System.out.printf("Queue Stress Finished: %d ops in %d ms (Avg: %.2f ops/sec)%n",
                          successCount.get(),
                          (end - start),
                          (successCount.get() / ((end - start) / 1000.0)));

        executor.shutdown();
    }

    @Test
    void testLockPermissionStress() throws Exception {
        String lockKey = "permission_stress";
        client.createKeyValue(lockKey, "data".getBytes()).get();

        // 1. Owner locks the object
        client.lockObject(lockKey, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();

        // 2. 32 threads try to "break" the lock simultaneously
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        AtomicInteger blockedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 1000; j++) {
                        try {
                            // Intruder (ID 999) tries to update a write-locked object
                            client.updateKeyValue(lockKey, "fail".getBytes(), 999).get();
                        } catch (ExecutionException e) {
                            blockedCount.incrementAndGet();
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        // We expect exactly THREAD_COUNT * 1000 blocked operations
        assertEquals(THREAD_COUNT * 1000, blockedCount.get(), "Some intruders bypassed the lock!");
        executor.shutdown();
    }
}