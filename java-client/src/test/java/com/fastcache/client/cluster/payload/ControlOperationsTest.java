package com.fastcache.client.cluster.payload;

import com.fastcache.TestBaseCluster;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class ControlOperationsTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testTtlMethodsCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        
        // Create should be done on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(key, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // All other access should be done on backup
        Boolean success = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).setTtl(key,keyHint, 100).get();
        Assertions.assertTrue(success);

        // Verify TTL
        Long res = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getTtl(key,keyHint).get();
        Assertions.assertTrue(res > 0 && res <= 100);
    }

    @Test
    void testTtlMethodsCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        
        // Create should be done on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(key, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // All other access should be done on master
        Boolean success = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).setTtl(key,keyHint, 100).get();
        Assertions.assertTrue(success);

        // Verify TTL
        Long res = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getTtl(key,keyHint).get();
        Assertions.assertTrue(res > 0 && res <= 100);
    }

    @Test
    void testLockingMechanismCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] lockKey = createLargePayload(KEY_SIZE);
        
        // Create should be done on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // All other access should be done on backup
        // Client 1 acquires lock
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey,keyHint, LockType.WRITE_LOCK, 101, Duration.ofSeconds(30)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // Client 2 attempts to lock (should fail based on server logic)
        LockStatus lockResConflict = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey,keyHint, LockType.WRITE_LOCK, 102, Duration.ofSeconds(30)).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict);
    }

    @Test
    void testLockingMechanismCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] lockKey = createLargePayload(KEY_SIZE);
        
        // Create should be done on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // All other access should be done on master
        // Client 1 acquires lock
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey,keyHint, LockType.WRITE_LOCK, 101, Duration.ofSeconds(30)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // Client 2 attempts to lock (should fail based on server logic)
        LockStatus lockResConflict = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey,keyHint, LockType.WRITE_LOCK, 102, Duration.ofSeconds(30)).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict);
    }
}