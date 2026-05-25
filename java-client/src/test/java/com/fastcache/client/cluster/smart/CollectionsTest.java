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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CollectionsTest extends TestBaseCluster {

    @Test
    void testListEdgeOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String listKey = "testVector" + UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createVector(listKey, List.of("middle".getBytes()))
                .get(); // Assume server allows create as list or use createList
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(listKey, List.of("tail".getBytes())).get();

        // Get Position
        byte[] posVal = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getElementAtPosition(listKey, 1).get();
        Assertions.assertEquals("tail", new String(posVal));

        // Remove Head
        Boolean headRemoved = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeHead(listKey).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testListEdgeOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String listKey = "testVector"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createVector(listKey, List.of("middle".getBytes()))
                .get(); // Assume server allows create as list or use createList
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(listKey, List.of("tail".getBytes())).get();

        // Get Position
        byte[] posVal = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getElementAtPosition(listKey, 1).get();
        Assertions.assertEquals("tail", new String(posVal));

        // Remove Head
        Boolean headRemoved = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeHead(listKey).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreamingCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createList(rangeKey, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(rangeKey, List.of(String.valueOf(i).getBytes())).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamElementInRange(rangeKey, false, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0)));
    }

    @Test
    void testRangeStreamingCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createList(rangeKey, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(rangeKey, List.of(String.valueOf(i).getBytes())).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamElementInRange(rangeKey, false, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0)));
    }

    @Test
    void testRangeStreamingVectorCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeVector"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createVector(rangeKey, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(rangeKey, List.of(String.valueOf(i).getBytes())).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamElementInRange(rangeKey, true, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0)));
    }

    @Test
    void testRangeStreamingVectorCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeVector"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createVector(rangeKey, List.of("0".getBytes())).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        for (int i = 1; i < 10; i++) {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(rangeKey, List.of(String.valueOf(i).getBytes())).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamElementInRange(rangeKey, true, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0)));
    }

    @Test
    void testCreateAndStreamListCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "listTestKey"+ UUID.randomUUID();
        String val1 = "item1";
        String val2 = "item2";

        // Create List with first element on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createList(key, List.of(val1.getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key, List.of(val2.getBytes(StandardCharsets.UTF_8))).get();

        List<String> results = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamList(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamListCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "listTestKey"+ UUID.randomUUID();
        String val1 = "item1";
        String val2 = "item2";

        // Create List with first element on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createList(key, List.of(val1.getBytes(StandardCharsets.UTF_8)))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key, List.of(val2.getBytes(StandardCharsets.UTF_8)),keyHint).get();

        List<String> results = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamList(key,keyHint).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVectorCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createVector(key, List.of("v1".getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key, List.of("v2".getBytes(StandardCharsets.UTF_8))).get();

        List<String> results = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamVector(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testCreateAndStreamVectorCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createVector(key, List.of("v1".getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key, List.of("v2".getBytes(StandardCharsets.UTF_8))).get();

        List<String> results = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamVector(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testFrontBackOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey"+ UUID.randomUUID();
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createList(key, List.of("head".getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key, List.of("tail".getBytes(StandardCharsets.UTF_8))).get();

        // Get Head/Front
        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(key).get();
        byte[] front = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getFront(key).get();
        Assertions.assertEquals("head", new String(head));
        Assertions.assertEquals("head", new String(front));

        // Get Tail
        byte[] tail = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getTail(key).get();
        Assertions.assertEquals("tail", new String(tail));
    }

    @Test
    void testFrontBackOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey"+ UUID.randomUUID();
        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createList(key, List.of("head".getBytes(StandardCharsets.UTF_8))).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key, List.of("tail".getBytes(StandardCharsets.UTF_8))).get();

        // Get Head/Front
        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(key).get();
        byte[] front = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getFront(key).get();
        Assertions.assertEquals("head", new String(head));
        Assertions.assertEquals("head", new String(front));

        // Get Tail
        byte[] tail = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getTail(key).get();
        Assertions.assertEquals("tail", new String(tail));
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        String key = "removalTestKey"+ UUID.randomUUID();
        client.createList(key, List.of("item1".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTail(key, List.of("item2".getBytes(StandardCharsets.UTF_8))).get();

        // Remove Front
        byte[] removed = client.getAndRemoveFront(key).get();
        Assertions.assertEquals("item1", new String(removed));

        // Verify tail is now head
        byte[] newHead = client.getFront(key).get();
        Assertions.assertEquals("item2", new String(newHead));
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        String key = "posTestKeyVector"+ UUID.randomUUID();
        client.createVector(key, List.of("pos0".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTail(key,
                                Arrays.asList("pos1".getBytes(StandardCharsets.UTF_8),
                                              "pos2".getBytes(StandardCharsets.UTF_8))).get();

        // Get At Position 1
        byte[] pos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertEquals("pos1", new String(pos1));

        // Remove At Position 1
        byte[] removed = client.getAndRemoveElementAtPosition(key, 1).get();
        Assertions.assertEquals("pos1", new String(removed));

        CountDownLatch latch = new CountDownLatch(1);
        List<byte[]> results = client.streamVector(key).get();

        latch.await(5, TimeUnit.SECONDS);
        System.out.println(results);

        // Verify Shift
        byte[] newPos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertEquals("pos2", new String(newPos1));
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        String key = "posTestKeyList"+ UUID.randomUUID();
        client.createList(key, List.of("pos0".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTail(key,
                                Arrays.asList("pos1".getBytes(StandardCharsets.UTF_8),
                                              "pos2".getBytes(StandardCharsets.UTF_8))).get();

        // Get At Position 1
        byte[] pos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertEquals("pos1", new String(pos1));

        // Remove At Position 1
        byte[] removed = client.getAndRemoveElementAtPosition(key, 1).get();
        Assertions.assertEquals("pos1", new String(removed));

        List<byte[]> results = client.streamList(key).get();

        System.out.println(results);
        // Verify Shift
        byte[] newPos1 = client.getElementAtPosition(key, 1).get();
        Assertions.assertEquals("pos2", new String(newPos1));
    }

    @Test
    void testCollectionNotFound() {
        String key = "nonExistentCollection"+ UUID.randomUUID();
        try {
            client.getFront(key).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Server should return NOT_FOUND if key doesn't exist
            Assertions.assertTrue(cause.getStatus().getCode() == Status.Code.NOT_FOUND
                                  || cause.getStatus().getCode() == Status.Code.INTERNAL);
        } catch (InterruptedException e) {
            Assertions.fail(e.getMessage());
        }
    }
}