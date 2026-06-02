package com.fastcache.client.cluster.migration;

import com.fastcache.TestBaseCluster;
import com.fastcache.client.cluster.AdvancedTest;
import com.fastcache.grpc.KeyHint;
import com.fastcache.utils.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class RawValuesTestImproved extends AdvancedTest {




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


}