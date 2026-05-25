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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CollectionsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testListEdgeOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createVector(listKey, List.of(zero))
                .get();// Assume server allows create as list or use createList
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(listKey, keyHint, List.of(one)).get();

        // Get Position
        byte[] posVal = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getElementAtPosition(listKey,keyHint, 1).get();
        Assertions.assertArrayEquals(one, posVal);

        // Remove Head
        Boolean headRemoved = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).removeHead(listKey,keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testListEdgeOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] listKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createVector(listKey, List.of(zero))
                .get();// Assume server allows create as list or use createList
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(listKey, keyHint, List.of(one)).get();

        // Get Position
        byte[] posVal = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getElementAtPosition(listKey,keyHint, 1).get();
        Assertions.assertArrayEquals(one, posVal);

        // Remove Head
        Boolean headRemoved = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).removeHead(listKey,keyHint).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreamingCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createList(rangeKey, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3=null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i == 2 ) item3=item;
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(rangeKey, keyHint, List.of(
                    item)).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamElementInRange(rangeKey,keyHint, false, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertArrayEquals(item3, rangeData.getFirst());
    }

    @Test
    void testRangeStreamingCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createList(rangeKey, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null ;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i ==2) item3 = item;
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(rangeKey, keyHint, List.of(
                    item)).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamElementInRange(rangeKey,keyHint, false, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertArrayEquals(item3, rangeData.getFirst());
    }

    @Test
    void testRangeStreamingVectorCreateOnMasterValidateOnBackup() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createVector(rangeKey, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte[] item3 = null;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i==2) item3=item;
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(rangeKey, keyHint, List.of(
                    item)).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamElementInRange(rangeKey,keyHint, true, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertArrayEquals(item3, rangeData.getFirst());
    }

    @Test
    void testRangeStreamingVectorCreateOnBackupValidateOnMaster() throws InterruptedException, ExecutionException {
        byte[] rangeKey = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createVector(rangeKey, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        byte [] item3=null ;
        for (int i = 1; i < 10; i++) {
            byte[] item = createLargePayload(VALUE_SIZE);
            if (i == 2) item3 =item ;
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(rangeKey, keyHint, List.of(
                    item)).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamElementInRange(rangeKey,keyHint, true, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertArrayEquals(item3, rangeData.getFirst());
    }

    @Test
    void testCreateAndStreamListCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] val1 = createLargePayload(VALUE_SIZE);
        byte[] val2 = createLargePayload(VALUE_SIZE);

        // Create List with first element on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createList(key, List.of(val1))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key,keyHint, List.of(val2)).get();

        List<byte[]> results = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamList(key,keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(val1, results.get(0));
        Assertions.assertArrayEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamListCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] val1 = createLargePayload(VALUE_SIZE);
        byte[] val2 = createLargePayload(VALUE_SIZE);

        // Create List with first element on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createList(key, List.of(val1))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add second element on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key,keyHint, List.of(val2)).get();

        List<byte[]> results = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamList(key,keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(val1, results.get(0));
        Assertions.assertArrayEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVectorCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createVector(key, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key, keyHint, List.of(one)).get();

        List<byte[]> results = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).streamVector(key,keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(zero, results.getFirst());
    }

    @Test
    void testCreateAndStreamVectorCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createVector(key, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key, keyHint, List.of(one)).get();

        List<byte[]> results = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).streamVector(key,keyHint).get();

        Assertions.assertEquals(2, results.size());
        Assertions.assertArrayEquals(zero, results.getFirst());
    }

    @Test
    void testFrontBackOperationsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on master
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createList(key, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on backup
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(key, keyHint, List.of(one)).get();

        // Get Head/Front
        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getHead(key,keyHint).get();
        byte[] front = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getFront(key,keyHint).get();
        Assertions.assertArrayEquals(zero, head);
        Assertions.assertArrayEquals(zero, front);

        // Get Tail
        byte[] tail = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getTail(key,keyHint).get();
        Assertions.assertArrayEquals(one, tail);
    }

    @Test
    void testFrontBackOperationsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        // Create on backup
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createList(key, List.of(zero))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);
        // Add on master
        byte[] one = createLargePayload(VALUE_SIZE);
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(key, keyHint, List.of(one)).get();

        // Get Head/Front
        byte[] head = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getHead(key,keyHint).get();
        byte[] front = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getFront(key,keyHint).get();
        Assertions.assertArrayEquals(zero, head);
        Assertions.assertArrayEquals(zero, front);

        // Get Tail
        byte[] tail = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getTail(key,keyHint).get();
        Assertions.assertArrayEquals(one, tail);
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] head = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.createList(key, List.of(head)).get();
        byte[] tail = createLargePayload(VALUE_SIZE);
        client.addElementToTail(key, keyHint, List.of(tail)).get();

        // Remove Front
        byte[] removedHead = client.getAndRemoveFront(key,keyHint).get();
        Assertions.assertArrayEquals(head, removedHead);

        // Verify tail is now head
        byte[] newHead = client.getFront(key,keyHint).get();
        Assertions.assertArrayEquals(tail, newHead);
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] init = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.createVector(key, List.of(init)).get();
        byte[] zero = createLargePayload(VALUE_SIZE);
        byte[] one = createLargePayload(VALUE_SIZE);
        client.addElementToTail(key, keyHint,
                                Arrays.asList(zero, one)).get();

        // Get At Position 1
        byte[] pos1 = client.getElementAtPosition(key,keyHint, 1).get();
        Assertions.assertArrayEquals(zero, pos1);

        // Remove At Position 1
        byte[] removed = client.getAndRemoveElementAtPosition(key,keyHint, 1).get();
        Assertions.assertArrayEquals(zero, removed);

        List<byte[]> results = client.streamVector(key,keyHint).get();

        System.out.println(results);

        // Verify Shift
        byte[] newPos1 = client.getElementAtPosition(key,keyHint, 1).get();
        Assertions.assertArrayEquals(one, newPos1);
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        byte[] zero = createLargePayload(VALUE_SIZE);
        KeyHint keyHint = client.createList(key, List.of(zero)).get();
        byte[] one = createLargePayload(VALUE_SIZE);
        byte[] two = createLargePayload(VALUE_SIZE);
        client.addElementToTail(key, keyHint,
                                Arrays.asList(one, two)).get();

        // Get At Position 1
        byte[] pos1 = client.getElementAtPosition(key,keyHint, 1).get();
        Assertions.assertArrayEquals(one, pos1);

        // Remove At Position 1
        byte[] removed = client.getAndRemoveElementAtPosition(key,keyHint, 1).get();
        Assertions.assertArrayEquals(one, removed);

        List<byte[]> results = client.streamList(key,keyHint).get();

        System.out.println(results);
        // Verify Shift
        byte[] newPos1 = client.getElementAtPosition(key,keyHint, 1).get();
        Assertions.assertArrayEquals(two, newPos1);
    }

    @Test
    void testCollectionNotFound() {
        byte[] key = createLargePayload(KEY_SIZE);
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