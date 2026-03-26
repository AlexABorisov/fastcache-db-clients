package com.fastcache.client;

import com.fastcache.grpc.KeyHintResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class RawValuesTest {

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
    void singleCreateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey";
        String testValue = "singleCreateValueValue";
        KeyHintResponse keyHintResponse = client.createKeyAsync(testKey, testValue.getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] bytes = client.getValueAsync(testKey).get();
        byte[] bytes1 = client.getValueAsync(testKey, keyHintResponse.getKeyHint()).get();
        Assertions.assertNotNull(keyHintResponse.getKeyHint());
        Assertions.assertEquals(testValue, new String(bytes1));
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertEquals(new String(bytes), new String(bytes1));
    }

    @Test
    void singleCreateExistValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistValue";
        String testValue = "singleCreateExistValue123";
        KeyHintResponse keyHintResponse = client.createKeyAsync(testKey, testValue.getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] bytes = client.getValueAsync(testKey).get();
        Boolean isExist = client.existAsync(testKey).get();
        Assertions.assertNotNull(keyHintResponse.getKeyHint());
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertTrue(isExist);
    }

    @Test
    void singleCreateExistHintValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistHintValue";
        String testValue = "singleCreateExistHintValue123";
        KeyHintResponse keyHintResponse = client.createKeyAsync(testKey, testValue.getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] bytes = client.getValueAsync(testKey).get();
        Boolean isExist = client.existAsync(testKey, keyHintResponse.getKeyHint()).get();
        Assertions.assertNotNull(keyHintResponse.getKeyHint());
        Assertions.assertEquals(testValue, new String(bytes));
        Assertions.assertTrue(isExist);
    }

    @Test
    void singleCreateGetAndDeleteValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateGetAndDeleteValue";
        String testValue = "singleCreateGetAndDeleteValue";
        KeyHintResponse keyHintResponse = client.createKeyAsync(testKey, testValue.getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] bytes = client.getAndDeleteValueAsync(testKey).get();
        Assertions.assertNotNull(keyHintResponse.getKeyHint());
        Assertions.assertEquals(testValue, new String(bytes));
        try {
            client.getValueAsync(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(cause.getStatus().getCode(), Status.Code.NOT_FOUND);
        }
    }

    @Test
    void singleGenNonExistValue() throws ExecutionException, InterruptedException {
        String testKey = "singleGenNonExistValue";
        try {
            client.getValueAsync(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(cause.getStatus().getCode(), Status.Code.NOT_FOUND);
        }
    }

    @Test
    void singleNonExistValue() throws ExecutionException, InterruptedException {
        String testKey = "singleNonExistValue";
        try {
            client.existAsync(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(cause.getStatus().getCode(), Status.Code.NOT_FOUND);
        }
    }

    @Test
    void singleCreateUpdateValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue";
        String testValue = "singleCreateUpdateValueValue";
        String testValueUpdate = "singleCreateUpdateValueValue123";
        KeyHintResponse keyHintResponse = client.createKeyAsync(testKey, testValue.getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] bytes = client.getValueAsync(testKey).get();
        Assertions.assertNotNull(keyHintResponse.getKeyHint());
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] oldVal = client.updateKeyAsync(testKey, testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        byte[] newVal = client.getValueAsync(testKey).get();
        Assertions.assertEquals(testValue, new String(oldVal));
        Assertions.assertEquals(testValueUpdate, new String(newVal));
    }

    @Test
    void singleCreateUpdateKeyHintValue() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateKeyHintValue";
        String testValue = "singleCreateUpdateKeyHintValue123";
        String testValueUpdate = "singleCreateUpdateKeyHintValue56543";
        KeyHintResponse keyHintResponse = client.createKeyAsync(testKey, testValue.getBytes(StandardCharsets.UTF_8))
                .get();
        byte[] bytes = client.getValueAsync(testKey).get();
        Assertions.assertNotNull(keyHintResponse.getKeyHint());
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] oldVal = client.updateKeyAsync(testKey,
                                              keyHintResponse.getKeyHint(),
                                              testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        byte[] newVal = client.getValueAsync(testKey).get();
        Assertions.assertEquals(testValue, new String(oldVal));
        Assertions.assertEquals(testValueUpdate, new String(newVal));
    }
}