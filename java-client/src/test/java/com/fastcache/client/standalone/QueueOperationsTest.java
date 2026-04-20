package com.fastcache.client.standalone;

import com.fastcache.client.FastCacheAsyncClient;
import com.fastcache.grpc.KeyHint;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class QueueOperationsTest extends TestBase {


    @Test
    void testQueueLifecycle() throws ExecutionException, InterruptedException {
        String qKey = "fifoQueue";
        String first = "message1";
        String second = "message2";

        // 1. createQueue with initial value
        KeyHint createRes = client.createQueue(qKey, List.of(first.getBytes(StandardCharsets.UTF_8))).get();
        Assertions.assertNotNull(createRes);

        // 2. addElementToTail
        boolean added = client.addElementToTail(qKey, List.of(second.getBytes(StandardCharsets.UTF_8)))
                .get();

        Assertions.assertTrue(added);

        // 3. getHead (Peek without removing)
        byte[] headData = client.getHead(qKey).get();
        Assertions.assertEquals(first, new String(headData));

        // 4. getAndRemoveFront (Atomic pop from head)
        byte[] popped = client.getAndRemoveFront(qKey).get();
        Assertions.assertEquals(first, new String(popped));

        // 5. Verify the new head is the second message
        byte[] newHeadData = client.getHead(qKey).get();
        Assertions.assertEquals(second, new String(newHeadData));

        // 6. removeHead (Delete without returning data)
        boolean removed = client.removeHead(qKey).get();
        Assertions.assertTrue(removed);

        // 7. Verify Queue is now empty or key doesn't exist

        byte[] empty = client.getHead(qKey).get();
        Assertions.assertEquals(0, empty.length);

    }

    @Test
    void testQueueOrderPersistence() throws ExecutionException, InterruptedException {
        String qKey = "orderTestQueue";
        client.createQueue(qKey, List.of("1".getBytes())).get();
        client.addElementToTail(qKey, Arrays.asList("2".getBytes(), "3".getBytes())).get();

        // FIFO verification: 1 -> 2 -> 3
        Assertions.assertEquals("1", new String(client.getAndRemoveFront(qKey).get()));
        Assertions.assertEquals("2", new String(client.getAndRemoveFront(qKey).get()));
        Assertions.assertEquals("3", new String(client.getAndRemoveFront(qKey).get()));
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