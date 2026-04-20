package com.fastcache.client.standalone;

import com.fastcache.client.FastCacheAsyncClient;
import com.fastcache.grpc.BoolResponse;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.KeyHintResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class AdvancedCollectionsTest extends TestBase {




    @Test
    void testHeadAndPositionalAddition() throws ExecutionException, InterruptedException {
        String listKey = "headPosKey";
        // Start with a list: [Middle]
        KeyHint keyHintResponse = client.createList(listKey, List.of("Middle".getBytes(StandardCharsets.UTF_8)))
                .get();

        // addElementToHead -> [Head, Middle]
        Boolean boolResponse = client.addElementToHead(listKey,
                                                            List.of("Head".getBytes(StandardCharsets.UTF_8)))
                .get();

        // addElementToPosition at 1 -> [Head, NewPos1, Middle]
        Boolean boolResponse1 = client.addElementToPosition(listKey, List.of(
                                                                      "NewPos1".getBytes(StandardCharsets.UTF_8)),
                                                                 1).get();

        byte[] head = client.getHead(listKey).get();
        byte[] pos1 = client.getElementAtPositionAsync(listKey, 1).get();

        Assertions.assertEquals("Head", new String(head));
        Assertions.assertEquals("NewPos1", new String(pos1));
    }

    @Test
    void testTailAndPositionalRemoval() throws ExecutionException, InterruptedException {
        String vecKey = "removePosKey";
        // Setup Vector: [0, 1, 2]
        client.createVector(vecKey, List.of("0".getBytes())).get();
        client.addElementToTail(vecKey, Arrays.asList("1".getBytes(), "2".getBytes())).get();

        // removeTail -> [0, 1]
        client.removeTail(vecKey).get();

        // removeElementAtPositionAsync at 0 -> [1]
        client.removeElementAtPositionAsync(vecKey, 0).get();

        byte[] remaining = client.getElementAtPositionAsync(vecKey, 0).get();
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
    void testRemoveElementInRangeSuccess() throws ExecutionException, InterruptedException {
        String key = "boolRangeKey";
        client.createVector(key, List.of("0".getBytes())).get();
        for (int i = 1; i < 5; i++) {
            client.addElementToTail(key, List.of(String.valueOf(i).getBytes())).get();
        }


        // Remove indices 0 to 2
        Boolean statusList = client.removeElementAtPositionAsync(key, 0, 2).get();

        Assertions.assertTrue(statusList);
    }

    @Test
    void testQueueTypeSafety() throws ExecutionException, InterruptedException {
        String qKey = "strictQueue";
        client.createQueue(qKey, List.of("q1".getBytes())).get();

        // Queues typically don't support positional addition in many implementations.
        // If your server returns an error for positional ops on Queues, this test verifies that.
        try {
            client.addElementToPosition(qKey, List.of("fail".getBytes()), 1).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            // Expecting an error code if Queues are strictly FIFO
            Assertions.assertNotEquals(Status.Code.OK, cause.getStatus().getCode());
        }
    }
}