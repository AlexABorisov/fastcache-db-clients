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

public class QueueOperationsTest extends TestBaseCluster {

    @Test
    void testQueueLifecycleCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue" + UUID.randomUUID();
        String first = "message1";
        String second = "message2";

        // 1. createQueue with initial value on master
        KeyHint createRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of(first.getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertNotNull(createRes);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on backup
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, List.of(second.getBytes(StandardCharsets.UTF_8))).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on backup
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey).get();
        Assertions.assertEquals(first, new String(headData));

        // 4. getAndRemoveFront (Atomic pop from head) on backup
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get();
        Assertions.assertEquals(first, new String(popped));

        // 5. Verify the new head is the second message on backup
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey).get();
        Assertions.assertEquals(second, new String(newHeadData));

        // 6. removeHead (Delete without returning data) on backup
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeHead(qKey).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on backup
        byte[] empty = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(qKey).get();
        Assertions.assertEquals(0, empty.length);
    }

    @Test
    void testQueueLifecycleCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue"+ UUID.randomUUID();
        String first = "message1";
        String second = "message2";

        // 1. createQueue with initial value on backup
        KeyHint createRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createQueue(qKey, List.of(first.getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertNotNull(createRes);
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 2. addElementToTail on master
        boolean added = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey, List.of(second.getBytes(StandardCharsets.UTF_8))).get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing) on master
        byte[] headData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey).get();
        Assertions.assertEquals(first, new String(headData));

        // 4. getAndRemoveFront (Atomic pop from head) on master
        byte[] popped = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get();
        Assertions.assertEquals(first, new String(popped));

        // 5. Verify the new head is the second message on master
        byte[] newHeadData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey).get();
        Assertions.assertEquals(second, new String(newHeadData));

        // 6. removeHead (Delete without returning data) on master
        boolean removed = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeHead(qKey).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist on master
        byte[] empty = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(qKey).get();
        Assertions.assertEquals(0, empty.length);
    }

    @Test
    void testQueueOrderPersistenceCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createQueue(qKey, List.of("1".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(qKey, Arrays.asList("2".getBytes(), "3".getBytes())).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get()));
        Assertions.assertEquals("2", new String(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get()));
        Assertions.assertEquals("3", new String(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndRemoveFront(qKey).get()));
    }

    @Test
    void testQueueOrderPersistenceCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createQueue(qKey, List.of("1".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(qKey, Arrays.asList("2".getBytes(), "3".getBytes())).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get()));
        Assertions.assertEquals("2", new String(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get()));
        Assertions.assertEquals("3", new String(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndRemoveFront(qKey).get()));
    }

    @Test
    void testQueueOperationsOnMissingKey() {
        String qKey = "missingQueue"+ UUID.randomUUID();

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