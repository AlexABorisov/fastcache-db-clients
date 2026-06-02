package com.fastcache.client.cluster;

import com.fastcache.TestBaseCluster;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AdvancedTest extends TestBaseCluster {
    public static final int KEY_SIZE = 1024;
    public static final int VALUE_SIZE = 2048;
    public static final int NUM_OF_KEYS = 100000;
    public static final int NUM_OF_KEYS_LEAK = 100;
    public static final long TTL = TimeUnit.MINUTES.toMillis(1);

    @FunctionalInterface
    public interface ThrowingValueCreator {
        KeyHint create(String key, byte[] valueBytes) throws Exception;
    }

    /**
     * Generic structural baseline loader.
     * Pass client calls via lambdas to dynamically provision values, queues, or vectors.
     */
    protected ConcurrentHashMap<String, Pair<String, KeyHint>> loadSynchronousLeakBaseline(ThrowingValueCreator valueCreator) {

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

    protected static <T> void runAsyncBatch(String operationName,
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

    protected void assertMigrationResults(String context, int completeSuccessCount) {
        System.out.println(context + " -> Success: " + completeSuccessCount + " Failure/Loss: " + (NUM_OF_KEYS
                                                                                                   - completeSuccessCount));
        Assertions.assertEquals(NUM_OF_KEYS, completeSuccessCount, context + " failed verification bounds.");
    }

    protected void assertLeakResults(String context, int completeSuccessCount) {
        System.out.println(context + " -> Success: " + completeSuccessCount + " Failure/Leak: " + (NUM_OF_KEYS_LEAK
                                                                                                   - completeSuccessCount));
        Assertions.assertEquals(NUM_OF_KEYS_LEAK,
                                completeSuccessCount,
                                context + " structural leakage verification failed.");
    }
}