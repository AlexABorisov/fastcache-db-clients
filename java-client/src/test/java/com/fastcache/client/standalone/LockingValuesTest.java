package com.fastcache.client.standalone;

import com.fastcache.client.FastCacheAsyncClient;
import com.fastcache.grpc.LockResponse;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import com.fastcache.grpc.UnlockResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LockingValuesTest {

    private final String lockKey = "existing_lock_object";
    private FastCacheAsyncClient client;

    @BeforeEach
    void init() throws ExecutionException, InterruptedException {
        client = new FastCacheAsyncClient("127.0.0.1", 50000);
        // Ensure the object exists in the cache before testing locks
        client.createKeyAsync(lockKey, "initial_data".getBytes(StandardCharsets.UTF_8)).get();
    }

    @AfterEach
    void stop() throws InterruptedException {
        // Clean up to ensure a fresh state for the next test
        try {
            client.removeAsync(lockKey).get();
        } catch (Exception ignored) {
        }
        client.shutdown();
    }

    @Test
    void testUnanimousLockAndAnyUnlock() throws ExecutionException, InterruptedException {
        // 1. Lock by anonymous/unanimous user (clientId = 0)
        LockResponse lockRes = client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 0, 60).get();
        Assertions.assertEquals(LockStatus.OK, lockRes.getResult(), "Should lock unanimously");

        // 2. Unlock by a different client (clientId = 999)
        // Logic: if lockedBy == 0, anyone can unlock.
        UnlockResponse unlockRes = client.unLockObjectAsync(lockKey, 999).get();
        Assertions.assertEquals(LockStatus.OK,
                                unlockRes.getResult(),
                                "Any client should be able to unlock a unanimous lock");
    }

    @Test
    void testSpecificLockAndRestrictedUnlock() throws ExecutionException, InterruptedException {
        int ownerId = 100;
        int intruderId = 200;

        // 1. Lock by specific owner
        LockResponse lockRes = client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, ownerId, 60).get();
        Assertions.assertEquals(LockStatus.OK, lockRes.getResult());

        // 2. Attempt unlock by intruder (Should be rejected)
        UnlockResponse failedUnlock = client.unLockObjectAsync(lockKey, intruderId).get();
        Assertions.assertEquals(LockStatus.CANT_UNLOCK,
                                failedUnlock.getResult(),
                                "Intruder should not be able to unlock owner's lock");

        // 3. Attempt unlock by owner (Should succeed)
        UnlockResponse successUnlock = client.unLockObjectAsync(lockKey, ownerId).get();
        Assertions.assertEquals(LockStatus.OK, successUnlock.getResult());
    }

    @Test
    void testLockingConflict() throws ExecutionException, InterruptedException {
        // 1. Client A locks the object
        client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 1, 60).get();

        // 2. Client B tries to lock the same object (Should fail)
        LockResponse conflictRes = client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 2, 60).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes.getResult(), "Should not allow double locking");
    }

    @Test
    void testUnanimousLockBlocksSpecificLock() throws ExecutionException, InterruptedException {
        // 1. Locked by 0
        client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 0, 60).get();

        // 2. Client 1 tries to lock specifically (Should fail because it's already locked)
        LockResponse conflictRes = client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 1, 60).get();
        Assertions.assertEquals(LockStatus.CANT_LOCK, conflictRes.getResult());
    }

    @Test
    void testUnlockOnExpiredObject() throws ExecutionException, InterruptedException {
        // 1. Lock with very short TTL
        client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 555, 1).get();

        // 2. Wait for TTL to expire on the i9 server
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        // 3. Try to unlock with a random ID
        // Logic: isLocked() is false, so canUnLock returns true (idempotency)
        UnlockResponse res = client.unLockObjectAsync(lockKey, 999).get();
        Assertions.assertEquals(LockStatus.OK, res.getResult(), "Unlock on expired lock should return OK");
    }

    @Test
    void testGlobalLockBlocksDataAccess() throws ExecutionException, InterruptedException {
        String key = "global_data_key";
        int ownerId = 1;
        int intruderId = 2;

        // 1. Create and then Lock Globally
        client.createKeyAsync(key, "sensitive_info".getBytes()).get();
        client.lockObjectAsync(key, LockType.GLOBAL, ownerId, 60).get();

        // 2. Owner should be able to read (requires clientId in GetRequest)
        // Assuming your getValueAsync is updated to pass clientId
        byte[] data = client.getValueAsync(key, ownerId).get();
        Assertions.assertNotNull(data);

        // 3. Intruder tries to read (Should fail)
        try {
            client.getValueAsync(key, intruderId).get();
            Assertions.fail("Intruder should have been blocked by GLOBAL lock");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            Assertions.assertEquals(Status.Code.PERMISSION_DENIED, cause.getStatus().getCode());
        }
    }
}