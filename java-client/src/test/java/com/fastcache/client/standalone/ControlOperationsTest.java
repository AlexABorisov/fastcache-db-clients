package com.fastcache.client.standalone;

import com.fastcache.client.FastCacheAsyncClient;
import com.fastcache.grpc.LockResponse;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import com.fastcache.grpc.TtlResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class ControlOperationsTest {
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
    void testTtlMethods() throws ExecutionException, InterruptedException {
        String key = "ttlKey";
        client.createKeyAsync(key, "data".getBytes()).get();

        // Set TTL to 100 seconds
        Boolean success = client.setTtlAsync(key, 100).get();
        Assertions.assertTrue(success);

        // Verify TTL
        TtlResponse res = client.getTtlAsync(key).get();
        Assertions.assertTrue(res.getTtl() > 0 && res.getTtl() <= 100);
    }

    @Test
    void testLockingMechanism() throws ExecutionException, InterruptedException {
        String lockKey = "resourceKey";
        client.createKeyAsync(lockKey, "secure_data".getBytes()).get();

        // Client 1 acquires lock
        LockResponse lockRes = client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 101, 30).get();
        Assertions.assertEquals(LockStatus.OK, lockRes.getResult());

        // Client 2 attempts to lock (should fail based on server logic)
        LockResponse lockResConflict = client.lockObjectAsync(lockKey, LockType.WRITE_LOCK, 102, 30).get();
        Assertions.assertNotEquals(LockStatus.OK, lockResConflict.getResult());
    }
}