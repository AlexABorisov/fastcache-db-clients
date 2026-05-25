package com.fastcache.client.cluster.smart;

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

public class QueueOperationsReplicationTest extends TestBaseCluster {

    @Test
    void testQueueLifecycleCreateOnMasterGetOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue"+ UUID.randomUUID();
        String first = "message1"+ UUID.randomUUID();
        String second = "message2"+ UUID.randomUUID();

        // 1. createQueue with initial value
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of(first.getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, List.of(second.getBytes(StandardCharsets.UTF_8)), keyHint).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey,keyHint).get();
        Assertions.assertEquals(first, new String(headData));

        // 4. getAndRemoveFront (Atomic pop from head)
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey, keyHint).get();
        Assertions.assertEquals(first, new String(popped));

        // 5. Verify the new head is the second message
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey, keyHint).get();
        Assertions.assertEquals(second, new String(newHeadData));

        // 6. removeHead (Delete without returning data)
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeHead(qKey).get();
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
        String qKey = "fifoQueue"+ UUID.randomUUID();
        String first = "message1"+ UUID.randomUUID();
        String second = "message2"+ UUID.randomUUID();

        // 1. createQueue with initial value
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of(first.getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertNotNull(keyHint);

        Thread.sleep(500);
        // 2. addElementToTail
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey, List.of(second.getBytes(StandardCharsets.UTF_8)), keyHint).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey).get();
        Assertions.assertEquals(first, new String(headData));

        // 4. getAndRemoveFront (Atomic pop from head)
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get();
        Assertions.assertEquals(first, new String(popped));

        // 5. Verify the new head is the second message
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey).get();
        Assertions.assertEquals(second, new String(newHeadData));

        // 6. removeHead (Delete without returning data)
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeHead(qKey).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist
        Thread.sleep(500);
        byte[] emptyB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey).get();
        byte[] emptyM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey).get();
        Assertions.assertEquals(0, emptyM.length);
        Assertions.assertEquals(0, emptyB.length);

    }

    @Test
    void testQueueOrderPersistenceCMGB() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of("1".getBytes())).get();
        Thread.sleep(500);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, Arrays.asList("2".getBytes(), "3".getBytes())).get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get()));
        Thread.sleep(500);
        Assertions.assertEquals("2", new String(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get()));
        Thread.sleep(500);
        Assertions.assertEquals("3", new String(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get()));
    }

    @Test
    void testQueueOrderPersistenceCBGM() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue" + UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createQueue(qKey, List.of("1".getBytes())).get();
        Thread.sleep(500);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey, Arrays.asList("2".getBytes(), "3".getBytes())).get();
        Thread.sleep(500);
        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get()));
        Thread.sleep(500);
        Assertions.assertEquals("2", new String(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get()));
        Thread.sleep(500);
        Assertions.assertEquals("3", new String(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get()));
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        String qKey = "missingQueue";

        // Test addElementToTail on non-existent key
        try {
            client.addElementToTail(qKey, List.of("data".getBytes())).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}