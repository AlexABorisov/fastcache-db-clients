package com.fastcache.client.cluster.leaks;

import com.fastcache.client.cluster.AdvancedTest;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorTestImproved extends AdvancedTest {






    @Test
    void createVectorValueLoopLeakTest1() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(vBytes)).get());

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

        assertLeakResults("createVectorValueLoopLeakTest1", good.get());

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

        assertLeakResults("createVectorValueLoopLeakTest2", good.get());
    }

    @Test
    void createVectorValueLoopLeakTest2() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(vBytes)).get());

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

        assertLeakResults("createVectorValueLoopLeakTest", good.get());

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

        assertLeakResults("createVectorValueLoopLeakTest", good.get());
    }


    @Test
    void createVectorValueLoopLeakTest3() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(vBytes)).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                byte[] data = client.getAndRemoveElementAtPosition(client.serializeKey(k), v.second,0).get();
                if (v.first.equals(new String(data, StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createVectorValueLoopLeakTest3", good.get());

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

        assertLeakResults("createVectorValueLoopLeakTest", good.get());
    }


    @Test
    void createListValueLoopLeakTest4() {
        // Easily test standard values by passing the creator lambda
        ConcurrentHashMap<String, Pair<String, KeyHint>> keyValueMap
                = loadSynchronousLeakBaseline((k, vBytes) -> client.createVector(k, List.of(vBytes)).get());

        AtomicInteger good = new AtomicInteger();
        keyValueMap.forEach((k, v) -> {
            try {
                byte[] data = client.getAndRemoveTail(client.serializeKey(k), v.second).get();
                if (v.first.equals(new String(data, StandardCharsets.UTF_8))) {
                    good.getAndIncrement();
                }
            } catch (Exception ignored) {
            }
        });

        assertLeakResults("createListValueLoopLeakTest3", good.get());

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

        assertLeakResults("createListValueLoopLeakTest", good.get());
    }
    /* ========================================================================
       Generic Code-Reuse Engines & Assertions
       ======================================================================== */


}