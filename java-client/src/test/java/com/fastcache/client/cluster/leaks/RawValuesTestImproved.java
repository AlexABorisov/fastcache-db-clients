package com.fastcache.client.cluster.leaks;

import com.fastcache.client.cluster.AdvancedTest;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RawValuesTestImproved extends AdvancedTest {




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




}