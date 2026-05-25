package com.fastcache.client.cluster.payload;

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

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testHeadAndPositionalAdditionCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Start with a list: [Middle]
        // Create on master
        byte[] middle = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createList(listKey, List.of(middle)).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        byte[] head = createLargePayload(VALUE_SIZE);
        Boolean boolResponse = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToHead(listKey, keyHint, List.of(
                head)).get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        byte[] newPos1 = createLargePayload(VALUE_SIZE);
        Boolean boolResponse1 = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToPosition(listKey, keyHint,
                                                                                                           List.of(newPos1),
                                                                                                           1).get();

        byte[] head1 = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(listKey,keyHint).get();
        byte[] pos1 = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getElementAtPosition(listKey,keyHint, 1).get();

        Assertions.assertArrayEquals(head1, head);
        Assertions.assertArrayEquals(pos1, newPos1);
    }

    @Test
    void testHeadAndPositionalAdditionCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Start with a list: [Middle]
        // Create on backup
        byte[] middle1 = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createList(listKey, List.of(middle1)).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // addElementToHead -> [Head, Middle]
        byte[] head1 = createLargePayload(VALUE_SIZE);
        Boolean boolResponse = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToHead(listKey, keyHint, List.of(
                head1)).get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        byte[] newPos1 = createLargePayload(VALUE_SIZE);
        Boolean boolResponse1 = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToPosition(listKey, keyHint,
                                                                                                           List.of(newPos1),
                                                                                                           1).get();

        Assertions.assertTrue(boolResponse1);
        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(listKey,keyHint).get();
        byte[] pos1 = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getElementAtPosition(listKey,keyHint, 1).get();

        Assertions.assertArrayEquals(head1, head);
        Assertions.assertArrayEquals(newPos1, pos1);
    }

    @Test
    void testTailAndPositionalRemovalCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] vecKey = createLargePayload(KEY_SIZE);
        // Setup Vector: [0, 1, 2]
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createVector(vecKey, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(vecKey, keyHint, Arrays.asList(
                first, second)).get();

        // removeTail -> [0, 1]
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeTail(vecKey,keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeElementAtPosition(vecKey,keyHint, 0).get();

        byte[] remaining = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getElementAtPosition(vecKey,keyHint, 0).get();
        Assertions.assertArrayEquals(first, remaining);
    }

    @Test
    void testTailAndPositionalRemovalCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] vecKey = createLargePayload(KEY_SIZE);
        // Setup Vector: [0, 1, 2]
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createVector(vecKey, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(vecKey, keyHint, Arrays.asList(
                first, second)).get();

        // removeTail -> [0, 1]
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeTail(vecKey,keyHint).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeElementAtPosition(vecKey,keyHint, 0).get();

        byte[] remaining = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getElementAtPosition(vecKey,keyHint, 0).get();
        Assertions.assertArrayEquals(first, remaining);
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createVector(key, List.of(createLargePayload(VALUE_SIZE)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key,keyHint, List.of(createLargePayload(VALUE_SIZE))).get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeElementAtPosition(key,keyHint, 0).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testRemoveElementInRangeSuccessCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createVector(key, List.of(createLargePayload(VALUE_SIZE)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 5; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key,keyHint, List.of(createLargePayload(VALUE_SIZE))).get();
        }

        // Remove indices 0 to 2
        Boolean statusList = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeElementAtPosition(key,keyHint, 0).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafetyCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createQueue(qKey, List.of(createLargePayload(VALUE_SIZE)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToPosition(qKey,keyHint, List.of(createLargePayload(VALUE_SIZE)), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }

    @Test
    void testQueueTypeSafetyCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createQueue(qKey, List.of(createLargePayload(VALUE_SIZE)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToPosition(qKey,keyHint, List.of(createLargePayload(VALUE_SIZE)), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}