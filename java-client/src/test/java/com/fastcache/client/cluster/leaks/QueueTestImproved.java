package com.fastcache.client.cluster.leaks;

import com.fastcache.client.cluster.AdvancedTest;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueTestImproved extends AdvancedTest {






    @Test
    void createQueueValueLoopLeakTest1() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createQueue(k, List.of(vBytes)).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                byte[] data = client.getHead(k, v.second).get();
                if (v.first.equals(new String(data, StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest1", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (b.booleanValue()) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest2", good.get());
    }

    @Test
    void createQueueValueLoopLeakTest2() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createQueue(k, List.of(vBytes)).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                byte[] data = client.getAndRemoveFront(k, v.second).get();
                if (v.first.equals(new String(data, StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest", good.get());

        AtomicInteger goodDelete = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                Boolean b = client.remove(k, v.second).get();
                if (b.booleanValue()) {
                    goodDelete.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createQueueValueLoopLeakTest", good.get());
    }


}