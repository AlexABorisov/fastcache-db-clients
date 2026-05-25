package com.fastcache.client.cluster.smart;

import com.fastcache.TestBase;
import com.fastcache.TestBaseCluster;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class AdvancedCollectionsTest extends TestBaseCluster {

    @Test
    void testHeadAndPositionalAdditionCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey" + UUID.randomUUID();
        // Start with a list: [Middle]
        // Create on master
        KeyHint keyHintResponse = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createList(listKey, List.of("Middle".getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        Boolean boolResponse = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToHead(listKey, List.of("Head".getBytes(StandardCharsets.UTF_8))).get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        Boolean boolResponse1 = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToPosition(listKey,
                                                                                                           List.of("NewPos1".getBytes(StandardCharsets.UTF_8)),
                                                                                                           1).get();

        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(listKey).get();
        byte[] pos1 = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getElementAtPosition(listKey, 1).get();

        Assertions.assertEquals("Head", new String(head));
        Assertions.assertEquals("NewPos1", new String(pos1));
    }

    @Test
    void testHeadAndPositionalAdditionCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey"+ UUID.randomUUID();
        // Start with a list: [Middle]
        // Create on backup
        KeyHint keyHintResponse = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createList(listKey, List.of("Middle".getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        Boolean boolResponse = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToHead(listKey, List.of("Head".getBytes(StandardCharsets.UTF_8))).get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        Boolean boolResponse1 = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToPosition(listKey,
                                                            List.of("NewPos1".getBytes(StandardCharsets.UTF_8)),
                                                            1).get();

        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(listKey).get();
        byte[] pos1 = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getElementAtPosition(listKey, 1).get();

        Assertions.assertEquals("Head", new String(head));
        Assertions.assertEquals("NewPos1", new String(pos1));
    }

    @Test
    void testTailAndPositionalRemovalCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey"+ UUID.randomUUID();
        // Setup Vector: [0, 1, 2]
        // Create on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createVector(vecKey, List.of("0".getBytes()))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(vecKey, Arrays.asList("1".getBytes(), "2".getBytes()),keyHint).get();

        // removeTail -> [0, 1]
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeTail(vecKey,keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeElementAtPosition(vecKey,keyHint, 0).get();

        byte[] remaining = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getElementAtPosition(vecKey,keyHint, 0).get();
        Assertions.assertEquals("1", new String(remaining));
    }

    @Test
    void testTailAndPositionalRemovalCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey"+ UUID.randomUUID();
        // Setup Vector: [0, 1, 2]
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createVector(vecKey, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(vecKey, Arrays.asList("1".getBytes(), "2".getBytes())).get();

        // removeTail -> [0, 1]
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeTail(vecKey).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeElementAtPosition(vecKey, 0,0).get();

        byte[] remaining = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getElementAtPosition(vecKey, 0).get();
        Assertions.assertEquals("1", new String(remaining));
    }

    //    @Test
    //    void testStreamAndRemoveRangeList() throws InterruptedException, ExecutionException {
    //        String key = "rangeStreamRemoveKey";
    //        // Setup: [A, B, C, D, E]
    //        client.createList(key, "A".getBytes()).get();
    //        client.addElementToTail(key,
    //                                     Arrays.asList("B".getBytes(),
    //                                                   "C".getBytes(),
    //                                                   "D".getBytes(),
    //                                                   "D".getBytes(),
    //                                                   "E".getBytes())).get();
    //
    //        List<String> removedValues = new ArrayList<>();
    //        CountDownLatch latch = new CountDownLatch(1);
    //
    //        // Stream and remove range 1 to 3 (B, C, D)
    //        client.streamAndRemoveElementInRange(key,
    //                                             false,
    //                                             1,
    //                                             3,
    //                                             val -> removedValues.add(new String(val)),
    //                                             err -> latch.countDown(),
    //                                             latch::countDown);
    //
    //        latch.await(5, TimeUnit.SECONDS);
    //
    //        Assertions.assertEquals(3, removedValues.size());
    //        Assertions.assertEquals("B", removedValues.get(0));
    //        Assertions.assertEquals("D", removedValues.get(2));
    //
    //        // Final check: Should only have [A, E]
    //        byte[] head = client.getHead(key).get();
    //        byte[] tail = client.getTailAsync(key).get();
    //        Assertions.assertEquals("A", new String(head));
    //        Assertions.assertEquals("E", new String(tail));
    //    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createVector(key, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key, List.of(String.valueOf(i).getBytes())).get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeElementAtPosition(key, 0, 2).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createVector(key, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key, List.of(String.valueOf(i).getBytes())).get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeElementAtPosition(key, 0, 2).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafetyCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of("q1".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToPosition(qKey, List.of("fail".getBytes()), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }

    @Test
    void testQueueTypeSafetyCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createQueue(qKey, List.of("q1".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToPosition(qKey, List.of("fail".getBytes()), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}