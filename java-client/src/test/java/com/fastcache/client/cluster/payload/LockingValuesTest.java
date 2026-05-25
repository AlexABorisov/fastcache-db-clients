package com.fastcache.client.cluster.payload;

import com.fastcache.TestBase;
import com.fastcache.TestBaseCluster;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LockingValuesTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    @Test
    void testUnanimousLockAndAnyUnlockCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        // Ensure the object exists in the cache before testing locks
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        // Create should be done on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 1. Lock by anonymous/unanimous user (clientId = 0)
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes, "Should lock unanimously");

        // 2. Unlock by a different client (clientId = 999)
        // Logic: if lockedBy == 0, anyone can unlock.
        LockStatus unlockRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).unlockObject(lockKey1,keyHint, 999).get();
        Assertions.assertEquals(LockStatus.OK, unlockRes, "Any client should be able to unlock a unanimous lock");
    }

    @Test
    void testUnanimousLockAndAnyUnlockCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        // Ensure the object exists in the cache before testing locks
        byte[] lockKey1 = createLargePayload(KEY_SIZE);

        // Create should be done on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 1. Lock by anonymous/unanimous user (clientId = 0)
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes, "Should lock unanimously");

        // 2. Unlock by a different client (clientId = 999)
        // Logic: if lockedBy == 0, anyone can unlock.
        LockStatus unlockRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).unlockObject(lockKey1,keyHint, 999).get();
        Assertions.assertEquals(LockStatus.OK, unlockRes, "Any client should be able to unlock a unanimous lock");
    }

    @Test
    void testSpecificLockAndRestrictedUnlockCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        int ownerId = 100;
        int intruderId = 200;

        // Create should be done on master
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 1. Lock by specific owner
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // 2. Attempt unlock by intruder (Should be rejected)
        LockStatus failedUnlock = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).unlockObject(lockKey1,keyHint, intruderId).get();
        Assertions.assertEquals(LockStatus.CANT_UNLOCK,
                                failedUnlock,
                                "Intruder should not be able to unlock owner's lock");

        // 3. Attempt unlock by owner (Should succeed)
        LockStatus successUnlock = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).unlockObject(lockKey1,keyHint, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    void testSpecificLockAndRestrictedUnlockCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        int ownerId = 100;
        int intruderId = 200;

        // Create should be done on backup
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // 1. Lock by specific owner
        LockStatus lockRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // 2. Attempt unlock by intruder (Should be rejected)
        LockStatus failedUnlock = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).unlockObject(lockKey1,keyHint, intruderId).get();
        Assertions.assertEquals(LockStatus.CANT_UNLOCK,
                                failedUnlock,
                                "Intruder should not be able to unlock owner's lock");

        // 3. Attempt unlock by owner (Should succeed)
        LockStatus successUnlock = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).unlockObject(lockKey1,keyHint, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    void testLockingConflictCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {

        // Create should be done on master
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Client A locks on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();

        // 2. Client B tries to lock the same object (Should fail)
        LockStatus conflictRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 2, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes, "Should not allow double locking");
    }

    @Test
    void testLockingConflictCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        // 1. Client A locks the object

        // Create should be done on backup
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Client A locks on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();

        // 2. Client B tries to lock the same object (Should fail)
        LockStatus conflictRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 2, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes, "Should not allow double locking");
    }

    @Test
    void testUnanimousLockBlocksSpecificLockCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        // 1. Locked by 0

        // Create should be done on master
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Locked by 0 on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        // 2. Client 1 tries to lock specifically (Should fail because it's already locked)
        LockStatus conflictRes = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes);
    }

    @Test
    void testUnanimousLockBlocksSpecificLockCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        // 1. Locked by 0

        // Create should be done on backup
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Locked by 0 on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        // 2. Client 1 tries to lock specifically (Should fail because it's already locked)
        LockStatus conflictRes = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes);
    }

    @Test
    void testUnlockOnExpiredObjectCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        // 1. Lock with very short TTL

        // Create should be done on master
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Lock on backup
        LockStatus lockStatus = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 555, Duration.ofSeconds(1)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // 2. Wait for TTL to expire on the i9 server
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        // 3. Try to unlock with a random ID
        // Logic: isLocked() is false, so canUnLock returns true (idempotency)
        LockStatus res = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).unlockObject(lockKey1,keyHint, 999).get();
        Assertions.assertEquals(LockStatus.OK, res, "Unlock on expired lock should return OK");
    }

    @Test
    void testUnlockOnExpiredObjectCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        // 1. Lock with very short TTL

        // Create should be done on backup
        byte[] lockKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(lockKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Lock on master
        LockStatus lockStatus = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(lockKey1,keyHint, LockType.WRITE_LOCK, 555, Duration.ofSeconds(1)).get();
        Assertions.assertEquals(LockStatus.OK, lockStatus);

        // 2. Wait for TTL to expire on the i9 server
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        // 3. Try to unlock with a random ID
        // Logic: isLocked() is false, so canUnLock returns true (idempotency)
        LockStatus res = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).unlockObject(lockKey1,keyHint, 999).get();
        Assertions.assertEquals(LockStatus.OK, res, "Unlock on expired lock should return OK");
    }

    @Test
    void testGlobalLockBlocksDataAccessCreateOnMasterValidateOnBackup() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        int ownerId = 1;
        int intruderId = 2;

        // 1. Create and then Lock Globally
        // Create should be done on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(key, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(key,keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();

        // 2. Owner should be able to read (requires clientId in GetRequest)
        // Assuming your getValue is updated to pass clientId
        byte[] data = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(key,keyHint, ownerId).get();
        Assertions.assertNotNull(data);

        // 3. Intruder tries to read (Should fail)
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(key,keyHint, intruderId).get();
            Assertions.fail("Intruder should have been blocked by GLOBAL lock");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }
    }

    @Test
    void testGlobalLockBlocksDataAccessCreateOnBackupValidateOnMaster() throws ExecutionException, InterruptedException {
        byte[] key = createLargePayload(KEY_SIZE);
        int ownerId = 1;
        int intruderId = 2;

        // 1. Create and then Lock Globally
        // Create should be done on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(key, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(key,keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();

        // 2. Owner should be able to read (requires clientId in GetRequest)
        // Assuming your getValue is updated to pass clientId
        byte[] data = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(key,keyHint, ownerId).get();
        Assertions.assertNotNull(data);

        // 3. Intruder tries to read (Should fail)
        try {
            client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(key,keyHint, intruderId).get();
            Assertions.fail("Intruder should have been blocked by GLOBAL lock");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }
    }
}