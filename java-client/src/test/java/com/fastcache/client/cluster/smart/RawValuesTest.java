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
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class RawValuesTest extends TestBaseCluster {

    @Test
    void singleCreateValueMaster() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey"+ UUID.randomUUID();
        String testValue = "singleCreateValueValue"+ UUID.randomUUID();
        KeyHint hint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,hint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,hint).get();
        Assertions.assertNotNull(hint);
        Assertions.assertEquals(testValue, new String(bytesM));
        Assertions.assertEquals(testValue, new String(bytesB));
        Assertions.assertEquals(new String(bytesM), new String(bytesB));
    }

    @Test
    void singleCreateValueBackup() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateValueKey"+ UUID.randomUUID();
        String testValue = "singleCreateValueValue"+ UUID.randomUUID();
        KeyHint hint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,hint).get();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,hint).get();
        Assertions.assertNotNull(hint);
        Assertions.assertEquals(testValue, new String(bytesM));
        Assertions.assertEquals(testValue, new String(bytesB));
        Assertions.assertEquals(new String(bytesM), new String(bytesB));
    }


    @Test
    void singleCreateExistValueMaster() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistValue"+ UUID.randomUUID();
        String testValue = "singleCreateExistValue123"+ UUID.randomUUID();
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,KeyHint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,KeyHint).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey,KeyHint).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey,KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytesM));
        Assertions.assertEquals(testValue, new String(bytesB));
        Assertions.assertTrue(isExistM);
        Assertions.assertTrue(isExistB);
    }
    @Test
    void singleCreateExistValueBackup() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistValue" + UUID.randomUUID();
        String testValue = "singleCreateExistValue123"+ UUID.randomUUID();
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,KeyHint).get();
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,KeyHint).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey,KeyHint).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey,KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytesM));
        Assertions.assertEquals(testValue, new String(bytesB));
        Assertions.assertTrue(isExistM);
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistHintValueMaster() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistHintValue"+ UUID.randomUUID();
        String testValue = "singleCreateExistHintValue123"+ UUID.randomUUID();
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey, KeyHint).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytesM));
        Assertions.assertTrue(isExistM);
        Assertions.assertEquals(testValue, new String(bytesB));
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateExistHintValueBackup() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateExistHintValue"+ UUID.randomUUID();
        String testValue = "singleCreateExistHintValue123"+ UUID.randomUUID();
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(100);
        byte[] bytesM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey).get();
        Boolean isExistM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey, KeyHint).get();
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey).get();
        Boolean isExistB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey, KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytesM));
        Assertions.assertTrue(isExistM);
        Assertions.assertEquals(testValue, new String(bytesB));
        Assertions.assertTrue(isExistB);
    }

    @Test
    void singleCreateGetAndDeleteValueMaster() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateGetAndDeleteValue"+ UUID.randomUUID();
        String testValue = "singleCreateGetAndDeleteValue"+ UUID.randomUUID();
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        Thread.sleep(100);
        byte[] bytesB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndDeleteValue(testKey,KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytesB));
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getAndDeleteValue(testKey,KeyHint).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getAndDeleteValue(testKey,KeyHint).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
@Test
    void singleCreateGetAndDeleteValueBackup() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateGetAndDeleteValue"+ UUID.randomUUID();
        String testValue = "singleCreateGetAndDeleteValue"+ UUID.randomUUID();
        KeyHint KeyHint = client.createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.getAndDeleteValue(testKey).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        try {
            client.getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleGenNonExistValueMaster() throws InterruptedException {
        String testKey = "singleGenNonExistValue"+ UUID.randomUUID();
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
@Test
    void singleGenNonExistValueBackup() throws InterruptedException {
        String testKey = "singleGenNonExistValue"+ UUID.randomUUID();
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleNonExistValueMaster() throws InterruptedException {
        String testKey = "singleNonExistValue"+ UUID.randomUUID();
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }
@Test
    void singleNonExistValueBackup() throws InterruptedException {
        String testKey = "singleNonExistValue"+ UUID.randomUUID();
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).existKey(testKey).get();
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    void singleCreateUpdateValueMaster() throws ExecutionException, InterruptedException {
        String testKey = "singleCreateUpdateValue"+ UUID.randomUUID();
        String testValue = "singleCreateUpdateValueValue"+ UUID.randomUUID();
        String testValueUpdate = "singleCreateUpdateValueValue123"+ UUID.randomUUID();
        KeyHint KeyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey, testValue.getBytes(StandardCharsets.UTF_8)).get();
        byte[] bytes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,KeyHint).get();
        Assertions.assertNotNull(KeyHint);
        Assertions.assertEquals(testValue, new String(bytes));
        byte[] oldVal = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey, testValueUpdate.getBytes(StandardCharsets.UTF_8)).get();
        byte[] newValM = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey,KeyHint).get();
        Assertions.assertEquals(testValue, new String(oldVal));
        Assertions.assertEquals(testValueUpdate, new String(newValM));
        Thread.sleep(100);
        byte[] newValB = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey,KeyHint).get();
        Assertions.assertEquals(testValue, new String(oldVal));
        Assertions.assertEquals(testValueUpdate, new String(newValB));
    }


}