package com.fastcache.client.cluster.payload;

import com.fastcache.TestBaseCluster;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class QueueOperationsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testQueueLifecycleCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of(first)).get();
        KeyHint createRes = keyHint;
        Assertions.assertNotNull(createRes);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on backup
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey,keyHint, List.of(second)).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on backup
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, headData);

        // 4. getAndRemoveFront (Atomic pop from head) on backup
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, popped);

        // 5. Verify the new head is the second message on backup
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData);

        // 6. removeHead (Delete without returning data) on backup
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeHead(qKey,keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on backup
        byte[] empty = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        Assertions.assertEquals(0, empty.length);
    }

    @Test
    void testQueueLifecycleCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createQueue(qKey, List.of(first)).get();
        Assertions.assertNotNull(keyHint);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on master
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey,keyHint, List.of(second)).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on master
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, headData);

        // 4. getAndRemoveFront (Atomic pop from head) on master
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, popped);

        // 5. Verify the new head is the second message on master
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData);

        // 6. removeHead (Delete without returning data) on master
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeHead(qKey,keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on master
        byte[] empty = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertEquals(0, empty.length);
    }

    @Test
    void testQueueOrderPersistenceCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] initial = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createQueue(qKey, List.of(initial))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, keyHint, Arrays.asList(first,
                                                                                                            second)).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertArrayEquals(initial, client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get());
        Assertions.assertArrayEquals(first, client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get());
        Assertions.assertArrayEquals(second, client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get());
    }

    @Test
    void testQueueOrderPersistenceCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] initial = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createQueue(qKey, List.of(initial))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey, keyHint, Arrays.asList(first,
                                                                                                            second)).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertArrayEquals(initial, client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get());
        Assertions.assertArrayEquals(first, client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get());
        Assertions.assertArrayEquals(second, client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get());
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        byte[] qKey = createLargePayload(KEY_SIZE);

        // Test addElementToTail on non-existent key
        try {
            client.addElementToTail(qKey,null, List.of(createLargePayload(VALUE_SIZE))).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}