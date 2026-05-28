package com.fastcache.client.cluster.migration;

import com.fastcache.TestBaseCluster;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RawValuesTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;
    private static final int NUM_OF_KEYS = 100000;
    private static final int NUM_OF_KEYS_LEAK = 100;
    private static final long TTL = TimeUnit.MINUTES.toMillis(1); // ~= max ttl;

    @Test
    void createKeyValueLoopMigration() throws ExecutionException, InterruptedException {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();
        {
            CountDownLatch latch = new CountDownLatch(NUM_OF_KEYS);
            AtomicInteger count = new AtomicInteger();
            long startW = System.currentTimeMillis();
            for (int i = 0; i < NUM_OF_KEYS; ++i) {
                String key = createRandomString(KEY_SIZE);
                String value = createRandomString(VALUE_SIZE);
                client.createKeyValue(key, value.getBytes()).thenAccept(keyhint -> {
                    keyValueMap.put(key, Pair.of(value, keyhint));
                    latch.countDown();
                    count.incrementAndGet();
                }).exceptionally(e -> {
                    latch.countDown();
                    count.incrementAndGet();
                    return null;
                });
            }
            latch.await();
            long duration = System.currentTimeMillis() - startW;
            System.out.println("Finished in:" + duration + "(ms)");
            System.out.println("Perf " + ((double) NUM_OF_KEYS / (duration / 1000.d)) + " (ops)");
            System.out.println("Created " + count.get() + " keys");
            System.out.println("Created " + keyValueMap.size() + " keys");
            System.out.println("Created " + NUM_OF_KEYS + " keys");
        }
        System.out.println("Stop 1 element of cluster");
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        {
            System.out.println("Start Get on reduced Cluster");
            AtomicInteger good = new AtomicInteger();
            AtomicInteger bad = new AtomicInteger();
            CountDownLatch readlatch = new CountDownLatch(NUM_OF_KEYS);
            long startR = System.currentTimeMillis();
            keyValueMap.forEach((k, v) -> {
                client.getValue(k, v.second).thenAccept(res -> {
                    if (v.first.equals(new String(res))) {
                        good.getAndIncrement();
                    } else {
                        bad.getAndIncrement();
                    }
                    readlatch.countDown();
                }).exceptionally(e -> {
                    bad.getAndIncrement();
                    readlatch.countDown();
                    return null;
                });

            });
            readlatch.await();
            long duration = System.currentTimeMillis() - startR;
            System.out.println("Result of operations: Good:" + good + " Bad:" + bad);
            System.out.println("Finished in:" + duration + "(ms)");
            System.out.println("Perf " + ((double) NUM_OF_KEYS / (duration / 1000.d)) + " (ops)");
        }
        System.out.println("Start new element");
        Thread.sleep(TimeUnit.MINUTES.toMillis(1));
        {

            System.out.println("Start Get on growing Cluster");
            AtomicInteger good1 = new AtomicInteger();
            AtomicInteger bad1 = new AtomicInteger();
            CountDownLatch readlatch1 = new CountDownLatch(NUM_OF_KEYS);
            long startR = System.currentTimeMillis();
            keyValueMap.forEach((k, v) -> {
                client.getValue(k, v.second).thenAccept(res -> {
                    if (v.first.equals(new String(res))) {
                        good1.getAndIncrement();
                    } else {
                        bad1.getAndIncrement();
                    }
                    readlatch1.countDown();
                }).exceptionally(e -> {
                    bad1.getAndIncrement();
                    readlatch1.countDown();
                    return null;
                });

            });
            readlatch1.await();
            long duration = System.currentTimeMillis() - startR;
            System.out.println("Result of operations: Good:" + good1 + " Bad:" + bad1);
            System.out.println("Finished in:" + duration + "(ms)");
            System.out.println("Perf " + ((double) NUM_OF_KEYS / (duration / 1000.d)) + " (ops)");
        }
    }

    @Test
    void createKeyValueLoopLeakTest() throws ExecutionException, InterruptedException {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();

        for (int i = 0; i < NUM_OF_KEYS_LEAK; ++i) {
            String key = createRandomString(KEY_SIZE);
            String value = createRandomString(VALUE_SIZE);
            KeyHint keyHint = client.createKeyValue(key, value.getBytes()).get();
            keyValueMap.put(key, Pair.of(value, keyHint));
        }
        System.out.println("Created " + NUM_OF_KEYS_LEAK + " keys");

        AtomicInteger good = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                if (v.first.equals(new String(client.getAndDeleteValue(k, v.second).get()))) {
                    good.getAndIncrement();
                } else {
                    bad.getAndIncrement();
                }
            } catch (Exception e) {
                bad.incrementAndGet();
            }
        });
        System.out.println("Result of operations: Good:" + good + " Bad:" + bad);
    }


    @Test
    void createDeleteKeyValueLoopLeakTest() throws ExecutionException, InterruptedException {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();

        for (int i = 0; i < NUM_OF_KEYS_LEAK; ++i) {
            String key = createRandomString(KEY_SIZE);
            String value = createRandomString(VALUE_SIZE);
            KeyHint keyHint = client.createKeyValue(key, value.getBytes()).get();
            keyValueMap.put(key, Pair.of(value, keyHint));
        }
        System.out.println("Created " + NUM_OF_KEYS_LEAK + " keys");

        AtomicInteger good = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                if (client.remove(k, v.second).get()) {
                    good.getAndIncrement();
                } else {
                    bad.getAndIncrement();
                }
            } catch (Exception e) {
                bad.incrementAndGet();
            }
        });
        System.out.println("Result of operations: Good:" + good + " Bad:" + bad);
    }

    @Test
    void createUpdateKeyValueLoopLeakTest() throws ExecutionException, InterruptedException {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();

        for (int i = 0; i < NUM_OF_KEYS_LEAK; ++i) {
            String key = createRandomString(KEY_SIZE);
            String value = createRandomString(VALUE_SIZE);
            KeyHint keyHint = client.createKeyValue(key, value.getBytes()).get();
            keyValueMap.put(key, Pair.of(value, keyHint));
        }
        System.out.println("Created " + NUM_OF_KEYS_LEAK + " keys");
        {
            AtomicInteger good = new AtomicInteger();
            AtomicInteger bad = new AtomicInteger();
            keyValueMap.forEach((k, v) -> {
                try {
                    String value = createRandomString(VALUE_SIZE);
                    if (v.first.equals(new String(client.updateKeyValue(k, value.getBytes()).get()))) {
                        good.getAndIncrement();
                    } else {
                        bad.getAndIncrement();
                    }
                } catch (Exception e) {
                    bad.incrementAndGet();
                }
            });
            System.out.println("Result of operations: Good:" + good + " Bad:" + bad);
        }
        {
            AtomicInteger good = new AtomicInteger();
            AtomicInteger bad = new AtomicInteger();
            keyValueMap.forEach((k, v) -> {
                try {
                    if (client.remove(k, v.second).get()) {
                        good.getAndIncrement();
                    } else {
                        bad.getAndIncrement();
                    }
                } catch (Exception e) {
                    bad.incrementAndGet();
                }
            });
            System.out.println("Result of operations: Good:" + good + " Bad:" + bad);
        }
    }

    @Test
    void createTTLKeyValueLoopLeakTest() throws ExecutionException, InterruptedException {
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap = new ConcurrentHashMap<>();

        for (int i = 0; i < NUM_OF_KEYS_LEAK; ++i) {
            String key = createRandomString(KEY_SIZE);
            String value = createRandomString(VALUE_SIZE);
            KeyHint keyHint = client.createKeyValue(key, value.getBytes()).get();
            client.setTtl(client.serializeKey(key), keyHint, TTL).get();
            keyValueMap.put(key, Pair.of(value, keyHint));
        }
        System.out.println("Created " + NUM_OF_KEYS_LEAK + " keys");

        System.out.println("Waiting to expire TTL " + TTL + " ms");
        Thread.sleep(TTL + 5000);

        System.out.println("Checking after TTL expired  " + NUM_OF_KEYS_LEAK + " keys");

        AtomicInteger good = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                if (!client.existKey(k, v.second).get()) {
                    good.getAndIncrement();
                } else {
                    bad.getAndIncrement();
                }
            } catch (Exception e) {
                bad.incrementAndGet();
            }
        });
        System.out.println("Result of operations: Good:" + good + " Bad:" + bad);
    }
}