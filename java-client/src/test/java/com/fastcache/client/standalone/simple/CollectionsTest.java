package com.fastcache.client.standalone.simple;

import com.fastcache.TestBase;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CollectionsTest extends TestBase {

    @Test
    void testListEdgeOperations() throws ExecutionException, InterruptedException {
        String listKey = "testVector";
        client.createVector(listKey, List.of("middle".getBytes()))
                .get(); // Assume server allows create as list or use createList

        client.addElementToTail(listKey, List.of("tail".getBytes())).get();

        // Get Position
        byte[] posVal = client.getElementAtPosition(listKey, 1).get();
        Assertions.assertEquals("tail", new String(posVal));

        // Remove Head
        Boolean headRemoved = client.removeHead(listKey).get();
        Assertions.assertTrue(headRemoved);
    }

    @Test
    void testRangeStreaming() throws InterruptedException, ExecutionException {
        String rangeKey = "rangeList";
        client.createList(rangeKey, List.of("0".getBytes())).get();
        for (int i = 1; i < 10; i++) {
            client.addElementToTail(rangeKey, List.of(String.valueOf(i).getBytes())).get();
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
        client.createVector(rangeKey, List.of("0".getBytes())).get();
        for (int i = 1; i < 10; i++) {
            client.addElementToTail(rangeKey, List.of(String.valueOf(i).getBytes())).get();
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
        client.createList(key, List.of(val1.getBytes(StandardCharsets.UTF_8))).get();
        // Add second element
        client.addElementToTail(key, List.of(val2.getBytes(StandardCharsets.UTF_8))).get();

        List<String> results = client.streamList(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(val1, results.get(0));
        Assertions.assertEquals(val2, results.get(1));
    }

    @Test
    void testCreateAndStreamVector() throws ExecutionException, InterruptedException {
        String key = "vectorTestKey";
        client.createVector(key, List.of("v1".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTail(key, List.of("v2".getBytes(StandardCharsets.UTF_8))).get();

        List<String> results = client.streamVector(key).get().stream().map(String::new).toList();

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(results.contains("v1"));
    }

    @Test
    void testFrontBackOperations() throws ExecutionException, InterruptedException {
        String key = "edgeTestKey";
        client.createList(key, List.of("head".getBytes(StandardCharsets.UTF_8))).get();
        client.addElementToTail(key, List.of("tail".getBytes(StandardCharsets.UTF_8))).get();

        // Get Head/Front
        byte[] head = client.getHead(key).get();
        byte[] front = client.getFront(key).get();
        Assertions.assertEquals("head", new String(head));
        Assertions.assertEquals("head", new String(front));

        // Get Tail
        byte[] tail = client.getTail(key).get();
        Assertions.assertEquals("tail", new String(tail));
    }

    @Test
    void testAtomicRemoval() throws ExecutionException, InterruptedException {
        String key = "removalTestKey";
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
        String key = "posTestKeyVector";
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
        String key = "posTestKeyList";
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
        String key = "nonExistentCollection";
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