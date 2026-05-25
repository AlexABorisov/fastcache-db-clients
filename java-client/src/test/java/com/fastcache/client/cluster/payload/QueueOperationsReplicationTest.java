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

public class QueueOperationsReplicationTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testQueueLifecycleCreateOnMasterGetOnBackup() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of(first)).get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, keyHint, Arrays.asList(second)).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, headData);

        // 4. getAndRemoveFront (Atomic pop from head)
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertArrayEquals(first, popped);

        // 5. Verify the new head is the second message
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData);

        // 6. removeHead (Delete without returning data)
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeHead(qKey,keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        byte[] emptyB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        byte[] emptyM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertEquals(0, emptyM.length);
        Assertions.assertEquals(0, emptyB.length);

    }
    @Test
    void testQueueLifecycleCreateOnBackupGetOnMaster() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] first = createLargePayload(VALUE_SIZE);
        byte[] second = createLargePayload(VALUE_SIZE);

        // 1. createQueue with initial value
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of(first)).get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey,keyHint, List.of(second)).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, headData);

        // 4. getAndRemoveFront (Atomic pop from head)
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get();
        Assertions.assertArrayEquals(first, popped);

        // 5. Verify the new head is the second message
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertArrayEquals(second, newHeadData);

        // 6. removeHead (Delete without returning data)
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeHead(qKey,keyHint).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        byte[] emptyB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        byte[] emptyM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey,keyHint).get();
        Assertions.assertEquals(0, emptyM.length);
        Assertions.assertEquals(0, emptyB.length);

    }

    @Test
    void testQueueOrderPersistenceCMGB() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createQueue(qKey, List.of(zero))
                .get();
        Thread.sleep(500);
        byte[] one = createLargePayload(VALUE_SIZE);
        byte[] two = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, keyHint, Arrays.asList(one,
                                                                                                            two)).get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        byte[] three = createLargePayload(VALUE_SIZE);
        client.addElementToTail(qKey, keyHint, List.of(three)).get();
        Assertions.assertArrayEquals(zero, client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get());
        Thread.sleep(500);
        Assertions.assertArrayEquals(one, client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get());
        Thread.sleep(500);
        Assertions.assertArrayEquals(two, client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get());
    }

    @Test
    void testQueueOrderPersistenceCBGM() throws ExecutionException, InterruptedException {
        byte[] qKey = createLargePayload(KEY_SIZE);
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createQueue(qKey, List.of(zero))
                .get();
        Thread.sleep(500);
        byte[] one = createLargePayload(VALUE_SIZE);
        byte[] two = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey, keyHint, Arrays.asList(one,
                                                                                                            two)).get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        byte[] three = createLargePayload(VALUE_SIZE);
        client.addElementToTail(qKey, keyHint, List.of(three)).get();
        Assertions.assertArrayEquals(zero, client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get());
        Thread.sleep(500);
        Assertions.assertArrayEquals(one, client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey,keyHint).get());
        Thread.sleep(500);
        Assertions.assertArrayEquals(two, client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey,keyHint).get());
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