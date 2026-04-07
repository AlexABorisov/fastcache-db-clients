package com.fastcache.client.standalone;

import com.fastcache.client.FastCacheAsyncClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CollectionsTest {
    private FastCacheAsyncClient client;

    @BeforeEach
    void init() {
        client = new FastCacheAsyncClient("127.0.0.1", 50000);
    }

    @AfterEach
    void stop() throws InterruptedException {
        client.shutdown();
    }

    @Test
    void testListEdgeOperations() throws ExecutionException, InterruptedException {
        String listKey = "testVector";
        client.createVectorAsync(listKey, List.of("middle".getBytes()))
                .get(); // Assume server allows create as list or use createList

        client.addElementToTailAsync(listKey, List.of("tail".getBytes())).get();

        // Get Position
        byte[] posVal = client.getElementAtPositionAsync(listKey, 1).get();
        Assertions.assertEquals("tail", new String(posVal));

        // Remove Head
        Boolean headRemoved = client.removeHeadAsync(listKey).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreaming() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList";
        client.createListAsync(rangeKey, List.of("0".getBytes())).get();
        for (int i = 1; i < 10; i++) {
            client.addElementToTailAsync(rangeKey, List.of(String.valueOf(i).getBytes())).get();
        }



        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.streamElementInRange(rangeKey, false, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0)));
    }

    @Test
    void testRangeStreamingVector() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeVector";
        client.createVectorAsync(rangeKey, List.of("0".getBytes())).get();
        for (int i = 1; i < 10; i++) {
            client.addElementToTailAsync(rangeKey, List.of(String.valueOf(i).getBytes())).get();
        }

        // Get elements from index 2 to 5
        List<byte[]> rangeData = client.streamElementInRange(rangeKey, true, 2, 5).get();

        System.out.println(rangeData);
        Assertions.assertEquals(3, rangeData.size()); // 2, 3, 4, 5
        Assertions.assertEquals("2", new String(rangeData.get(0)));
    }

    @Test
    void testCreateAndStreamList() throws ExecutionException, InterruptedException {
        String key = "listTestKey";
        String val1 = "item1";
        String val2 = "item2";

        // Create List with first element
        client.createListAsync(key, List.of(val1.getBytes(StandardCharsets.UTF_8))).get();
        // Add second element
        client.addElementToTailAsync(key, List.of(val2.getBytes(StandardCharsets.UTF_8))).get();



        List<String> results = client.streamList(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVector() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey";
        client.createVectorAsync(key, List.of("v1".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTailAsync(key, List.of("v2".getBytes(StandardCharsets.UTF_8))).get();

        List<String> results = client.streamVector(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testFrontBackOperations() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey";
        client.createListAsync(key, List.of("head".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTailAsync(key, List.of("tail".getBytes(StandardCharsets.UTF_8))).get();

        // Get Head/Front
        byte[] head = client.getHeadAsync(key).get();
        byte[] front = client.getFrontAsync(key).get();
        Assertions.assertEquals("head", new String(head));
        Assertions.assertEquals("head", new String(front));

        // Get Tail
        byte[] tail = client.getTailAsync(key).get();
        Assertions.assertEquals("tail", new String(tail));
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        String key = "removalTestKey";
        client.createListAsync(key, List.of("item1".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTailAsync(key, List.of("item2".getBytes(StandardCharsets.UTF_8))).get();

        // Remove Front
        byte[] removed = client.getAndRemoveFrontAsync(key).get();
        Assertions.assertEquals("item1", new String(removed));

        // Verify tail is now head
        byte[] newHead = client.getFrontAsync(key).get();
        Assertions.assertEquals("item2", new String(newHead));
    }

    @Test
    void testPositionalOperationsVector() throws ExecutionException, InterruptedException {
        String key = "posTestKeyVector";
        client.createVectorAsync(key, List.of("pos0".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTailAsync(key,
                                     Arrays.asList("pos1".getBytes(StandardCharsets.UTF_8),
                                                   "pos2".getBytes(StandardCharsets.UTF_8))).get();

        // Get At Position 1
        byte[] pos1 = client.getElementAtPositionAsync(key, 1).get();
        Assertions.assertEquals("pos1", new String(pos1));

        // Remove At Position 1
        byte[] removed = client.getAndRemoveElementAtPositionAsync(key, 1).get();
        Assertions.assertEquals("pos1", new String(removed));

        CountDownLatch latch = new CountDownLatch(1);
        List<byte[]> results = client.streamVector(key).get();

        latch.await(5, TimeUnit.SECONDS);
        System.out.println(results);

        // Verify Shift
        byte[] newPos1 = client.getElementAtPositionAsync(key, 1).get();
        Assertions.assertEquals("pos2", new String(newPos1));
    }

    @Test
    void testPositionalOperationsList() throws ExecutionException, InterruptedException {
        String key = "posTestKeyList";
        client.createListAsync(key, List.of("pos0".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTailAsync(key,
                                     Arrays.asList("pos1".getBytes(StandardCharsets.UTF_8),
                                                   "pos2".getBytes(StandardCharsets.UTF_8))).get();

        // Get At Position 1
        byte[] pos1 = client.getElementAtPositionAsync(key, 1).get();
        Assertions.assertEquals("pos1", new String(pos1));

        // Remove At Position 1
        byte[] removed = client.getAndRemoveElementAtPositionAsync(key, 1).get();
        Assertions.assertEquals("pos1", new String(removed));


        List<byte[]> results = client.streamList(key).get();

        System.out.println(results);
        // Verify Shift
        byte[] newPos1 = client.getElementAtPositionAsync(key, 1).get();
        Assertions.assertEquals("pos2", new String(newPos1));
    }

    @Test
    void testCollectionNotFound() {
        String key = "nonExistentCollection";
        try {
            client.getFrontAsync(key).get();
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