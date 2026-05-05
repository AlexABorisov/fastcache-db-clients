package com.fastcache.client.cluster.smart;

import com.fastcache.TestBase;
import com.fastcache.TestBaseCluster;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class ControlOperationsTest extends TestBaseCluster {

    @Test
    void testTtlMethods() throws ExecutionException, InterruptedException {
        String key = "ttlKey";
        client.createKeyValue(key, "data".getBytes()).get();

        // Set TTL to 100 seconds
        Boolean success = client.setTtl(key, 100).get();
        Assertions.assertTrue(success);

        // Verify TTL
        Long res = client.getTtl(key).get();
        Assertions.assertTrue(res > 0 && res <= 100);
    }

    @Test
    void testLockingMechanism() throws ExecutionException, InterruptedException {
        String lockKey = "resourceKey";
        client.createKeyValue(lockKey, "secure_data".getBytes()).get();

        // Client 1 acquires lock
        LockStatus lockRes = client.lockObject(lockKey, LockType.WRITE_LOCK, 101, Duration.ofSeconds(30)).get();
        Assertions.assertEquals(LockStatus.OK, lockRes);

        // Client 2 attempts to lock (should fail based on server logic)
        LockStatus lockResConflict = client.lockObject(lockKey, LockType.WRITE_LOCK, 102, Duration.ofSeconds(30)).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict);
    }
}