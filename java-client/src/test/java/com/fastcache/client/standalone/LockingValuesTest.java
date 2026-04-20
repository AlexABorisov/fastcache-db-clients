package com.fastcache.client.standalone;

import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LockingValuesTest extends TestBase {

    private final String lockKey = "existing_lock_object";

    @Test
    void testUnanimousLockAndAnyUnlock() throws ExecutionException, InterruptedException {
        // Ensure the object exists in the cache before testing locks
        try {
            client.remove(lockKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(lockKey, "initial_data".getBytes(StandardCharsets.UTF_8)).get();

        // 1. Lock by anonymous/unanimous user (clientId = 0)
        LockStatus lockRes = client.lockObject(lockKey, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes, "Should lock unanimously");

        // 2. Unlock by a different client (clientId = 999)
        // Logic: if lockedBy == 0, anyone can unlock.
        LockStatus unlockRes = client.unlockObject(lockKey, 999).get();
        Assertions.assertEquals(LockStatus.OK,
                                unlockRes,
                                "Any client should be able to unlock a unanimous lock");
    }

    @Test
    void testSpecificLockAndRestrictedUnlock() throws ExecutionException, InterruptedException {
        int ownerId = 100;
        int intruderId = 200;
        try {
            client.remove(lockKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(lockKey, "initial_data".getBytes(StandardCharsets.UTF_8)).get();
        // 1. Lock by specific owner
        LockStatus lockRes = client.lockObject(lockKey, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // 2. Attempt unlock by intruder (Should be rejected)
        LockStatus failedUnlock = client.unlockObject(lockKey, intruderId).get();
        Assertions.assertEquals(LockStatus.CANT_UNLOCK,
                                failedUnlock,
                                "Intruder should not be able to unlock owner's lock");

        // 3. Attempt unlock by owner (Should succeed)
        LockStatus successUnlock = client.unlockObject(lockKey, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, successUnlock);
    }

    @Test
    void testLockingConflict() throws ExecutionException, InterruptedException {
        // 1. Client A locks the object
        try {
            client.remove(lockKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(lockKey, "initial_data".getBytes(StandardCharsets.UTF_8)).get();
        client.lockObject(lockKey, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();

        // 2. Client B tries to lock the same object (Should fail)
        LockStatus conflictRes = client.lockObject(lockKey, LockType.WRITE_LOCK, 2, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes, "Should not allow double locking");
    }

    @Test
    void testUnanimousLockBlocksSpecificLock() throws ExecutionException, InterruptedException {
        // 1. Locked by 0
        try {
            client.remove(lockKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(lockKey, "initial_data".getBytes(StandardCharsets.UTF_8)).get();
        client.lockObject(lockKey, LockType.WRITE_LOCK, 0, Duration.ofSeconds(60)).get();

        // 2. Client 1 tries to lock specifically (Should fail because it's already locked)
        LockStatus conflictRes = client.lockObject(lockKey, LockType.WRITE_LOCK, 1, Duration.ofSeconds(60)).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes);
    }

    @Test
    void testUnlockOnExpiredObject() throws ExecutionException, InterruptedException {
        // 1. Lock with very short TTL
        try {
            client.remove(lockKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(lockKey, "initial_data".getBytes(StandardCharsets.UTF_8)).get();
        LockStatus lockStatus = client.lockObject(lockKey, LockType.WRITE_LOCK, 555, Duration.ofSeconds(1)).get();
        Assertions.assertEquals(LockStatus.OK,lockStatus);

        // 2. Wait for TTL to expire on the i9 server
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        // 3. Try to unlock with a random ID
        // Logic: isLocked() is false, so canUnLock returns true (idempotency)
        LockStatus res = client.unlockObject(lockKey, 999).get();
        Assertions.assertEquals(LockStatus.OK, res, "Unlock on expired lock should return OK");
    }

    @Test
    void testGlobalLockBlocksDataAccess() throws ExecutionException, InterruptedException {
        String key = "global_data_key";
        int ownerId = 1;
        int intruderId = 2;
        try {
            client.remove(key,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        // 1. Create and then Lock Globally
        client.createKeyValue(key, "sensitive_info".getBytes()).get();
        client.lockObject(key, LockType.GLOBAL, ownerId, Duration.ofSeconds(60)).get();

        // 2. Owner should be able to read (requires clientId in GetRequest)
        // Assuming your getValue is updated to pass clientId
        byte[] data = client.getValue(key, ownerId).get();
        Assertions.assertNotNull(data);

        // 3. Intruder tries to read (Should fail)
        try {
            client.getValue(key, intruderId).get();
            Assertions.fail("Intruder should have been blocked by GLOBAL lock");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }
    }
}