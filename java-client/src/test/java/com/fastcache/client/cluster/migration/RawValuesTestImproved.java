package com.fastcache.client.cluster.migration;

import com.fastcache.TestBaseCluster;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class RawValuesTestImproved extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;
    private static final int NUM_OF_KEYS = 100000;
    private static final int NUM_OF_KEYS_LEAK = 100;
    private static final long TTL = TimeUnit.MINUTES.toMillis(1);

    /**
     * Functional interface allowing clean lambdas that throw exceptions
     * when creating different datatypes (Values, Vectors, Queues, Lists).
     */
    @FunctionalInterface
    private interface ThrowingValueCreator {
        KeyHint create(String key, byte[] valueBytes) throws Exception;
    }

    @Test
    void createKeyValueLoopMigration() throws InterruptedException {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();

        // Phase 1: High-Throughput Async Writes
        runAsyncBatch("Write Phase Loading", NUM_OF_KEYS, emitter -> {
            for (int i = 0; i < NUM_OF_KEYS; ++i) {
                emitter.accept(Pair.of(createRandomString(KEY_SIZE), createRandomString(VALUE_SIZE)));
            }
        }, (pair, goodCounter) -> {
            Pair<String, String> p = (Pair<String, String>) pair;
            client.createKeyValue(p.first, p.second.getBytes(StandardCharsets.UTF_8)).thenAccept(keyhint -> {
                keyValueMap.put(p.first, Pair.of(p.second, keyhint));
                goodCounter.incrementAndGet();
            });
        }, Pair.class);

        Assertions.assertEquals(NUM_OF_KEYS, keyValueMap.size(), "Mapping allocation dropped elements!");

        // Topology Change: Scale Down
        System.out.println(new Date() + " Stop 1 element of cluster");
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));

        // Phase 2: Async Read Validation on Degraded Cluster
        AtomicInteger reducedGood = new AtomicInteger();
        runAsyncBatch("Get on reduced Cluster",
                      NUM_OF_KEYS,
                      emitter -> keyValueMap.forEach((k, v) -> emitter.accept(Pair.of(k, v))),
                      (entry, badCounter) -> {
                          Pair<String, Pair<String, KeyHint>> p = (Pair<String, Pair<String, KeyHint>>) entry;
            client.getValue(p.first, p.second.second).thenAccept(res -> {
                              if (p.second.first.equals(new String(res, StandardCharsets.UTF_8))) {
                                  reducedGood.incrementAndGet();
                              } else {
                                  badCounter.incrementAndGet();
                              }
                          });
                      },
                      Pair.class);
        assertMigrationResults(new Date() + " Reduced Cluster Verifications", reducedGood.get());

        // Topology Change: Scale Up
        System.out.println(new Date() + " Start new element");
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));

        // Phase 3: Async Read Validation on Growing/Rebalancing Cluster
        AtomicInteger growingGood = new AtomicInteger();
        runAsyncBatch("Get on growing Cluster",
                      NUM_OF_KEYS,
                      emitter -> keyValueMap.forEach((k, v) -> emitter.accept(Pair.of(k, v))),
                      (entry, badCounter) -> {
                          Pair<String, Pair<String, KeyHint>> p = (Pair<String, Pair<String, KeyHint>>) entry;
                          client.getValue(p.first, p.second.second).thenAccept(res -> {
                              if (p.second.first.equals(new String(res, StandardCharsets.UTF_8))) {
                                  growingGood.incrementAndGet();
                              } else {
                                  badCounter.incrementAndGet();
                              }
                          });
                      },
                      Pair.class);
        assertMigrationResults("Growing Cluster Verifications", growingGood.get());
    }

    @Test
    void createKeyValueLoopLeakTest() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createKeyValue(k, vBytes).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                byte[] data = client.getAndDeleteValue(k, v.second).get();
                if (v.first.equals(new String(data, StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("GetAndDeleteLeakTest", good.get());
    }

    @Test
    void createDeleteKeyValueLoopLeakTest() {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createKeyValue(k, vBytes).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                if (Boolean.TRUE.equals(client.remove(k, v.second).get())) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("DeleteLoopLeakTest", good.get());
    }

    @Test
    void createUpdateKeyValueLoopLeakTest() {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createKeyValue(k, vBytes).get());

        AtomicInteger updateGood = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                String newValue = createRandomString(VALUE_SIZE);
                byte[] data = client.updateKeyValue(k, newValue.getBytes(StandardCharsets.UTF_8)).get();
                if (v.first.equals(new String(data, StandardCharsets.UTF_8))) {
                    updateGood.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });
        assertLeakResults("UpdateLoopPhase1", updateGood.get());

        AtomicInteger removeGood = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                if (Boolean.TRUE.equals(client.remove(k, v.second).get())) {
                    removeGood.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });
        assertLeakResults("UpdateLoopPhase2", removeGood.get());
    }

    @Test
    void createTTLKeyValueLoopLeakTest() throws InterruptedException {
        // Can utilize different engine collection calls here if expanding to queues/vectors
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = loadSynchronousLeakBaseline((k, vBytes) -> {
            KeyHint keyHint = client.createKeyValue(k, vBytes).get();
            client.setTtl(client.serializeKey(k), keyHint, TTL).get();
            return keyHint;
        });

        System.out.println("Waiting to expire TTL " + TTL + " ms");
        Thread.sleep(TTL + 5000);
        System.out.println("Checking after TTL expired  " + NUM_OF_KEYS_LEAK + " elements");

        AtomicInteger cleanlyExpiredCount = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                if (Boolean.FALSE.equals(client.existKey(k, v.second).get())) {
                    cleanlyExpiredCount.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("TTLExpirationLeakTest", cleanlyExpiredCount.get());
    }

    /* ========================================================================
       Generic Code-Reuse Engines & Assertions
       ======================================================================== */

    /**
     * Generic structural baseline loader.
     * Pass client calls via lambdas to dynamically provision values, queues, or vectors.
     */
    private ConcurrentHashMap<String, Pair<String, KeyHint>> loadSynchronousLeakBaseline(ThrowingValueCreator valueCreator) {

        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();
        for (int i = 0; i < NUM_OF_KEYS_LEAK; ++i) {
            String key = createRandomString(KEY_SIZE);
            String value = createRandomString(VALUE_SIZE);

            try {
                // Evaluates your custom target lambda dynamically
                KeyHint keyHint = valueCreator.create(key, value.getBytes(StandardCharsets.UTF_8));
                keyValueMap.put(key, Pair.of(value, keyHint));
            } catch (Exception e) {
                Assertions.fail("Baseline cluster injection initialization failed!", e);
            }
        }
        System.out.println("Synchronously initialized " + NUM_OF_KEYS_LEAK + " cluster entities.");
        return keyValueMap;
    }

    private static <T> void runAsyncBatch(String operationName,
                                          int totalExpectedTasks,
                                          Consumer<Consumer<T>> sourceEmitter,
                                          BiConsumer<T, AtomicInteger> taskDriver,
                                          Class<T> tClass) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(totalExpectedTasks);
        AtomicInteger metricsCounter = new AtomicInteger();
        long startTime = System.currentTimeMillis();

        sourceEmitter.accept(item -> {
            try {
                taskDriver.accept(tClass.cast(item), metricsCounter);
            } finally {
                latch.countDown();
            }
        });

        latch.await();
        long duration = System.currentTimeMillis() - startTime;
        double secs = duration / 1000.0;

        System.out.println(new Date() + " [" + operationName + "] Finished in " + duration + " ms");
        System.out.println(new Date() + "[" + operationName + "] Throughput Profile: " + (secs > 0
                                                                             ? (totalExpectedTasks / secs)
                                                                             : totalExpectedTasks) + " ops");
    }

    private void assertMigrationResults(String context, int completeSuccessCount) {
        System.out.println(context + " -> Success: " + completeSuccessCount + " Failure/Loss: " + (NUM_OF_KEYS
                                                                                                   - completeSuccessCount));
        Assertions.assertEquals(NUM_OF_KEYS, completeSuccessCount, context + " failed verification bounds.");
    }

    private void assertLeakResults(String context, int completeSuccessCount) {
        System.out.println(context + " -> Success: " + completeSuccessCount + " Failure/Leak: " + (NUM_OF_KEYS_LEAK
                                                                                                   - completeSuccessCount));
        Assertions.assertEquals(NUM_OF_KEYS_LEAK,
                                completeSuccessCount,
                                context + " structural leakage verification failed.");
    }
}