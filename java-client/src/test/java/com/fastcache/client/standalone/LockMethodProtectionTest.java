package com.fastcache.client.standalone;

import com.fastcache.client.FastCacheAsyncClient;
import com.fastcache.grpc.LockResponse;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LockMethodProtectionTest {

    private final String testKey = "lock_protected_item";
    private final int ownerId = 1;
    private final int intruderId = 2;
    private FastCacheAsyncClient client;

    @BeforeEach
    void init() throws ExecutionException, InterruptedException {
        // Initialize client with a default ID
        client = new FastCacheAsyncClient("127.0.0.1", 50000, ownerId);
        // Ensure object exists
        client.createKeyAsync(testKey, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
    }

    @AfterEach
    void stop() throws InterruptedException {
        try {
            client.removeAsync(testKey, ownerId).get();
        } catch (Exception ignored) {
        }
        client.shutdown();
    }

    // --- SECTION 1: GLOBAL LOCK PROTECTION ---

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others")
    void testGlobalLockUnaryProtection() throws Exception {
        // Owner locks GLOBAL
        client.lockObjectAsync(testKey, LockType.GLOBAL, ownerId, 30).get();

        // 1. Intruder tries getValue
        assertPermissionDenied(() -> client.getValueAsync(testKey, intruderId).get());

        // 2. Intruder tries updateValue
        assertPermissionDenied(() -> client.updateKeyAsync(testKey, "new".getBytes(), intruderId).get());

        // 3. Intruder tries remove
        assertPermissionDenied(() -> client.removeAsync(testKey, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others")
    void testGlobalLockCollectionProtection() throws Exception {
        String listKey = "global_list";
        client.createListAsync(listKey, List.of("item1".getBytes())).get();
        client.lockObjectAsync(listKey, LockType.GLOBAL, ownerId, 30).get();

        // 1. Intruder tries getFront
        assertPermissionDenied(() -> client.getFrontAsync(listKey, intruderId).get());

        // 2. Intruder tries addElementToTail
        assertPermissionDenied(() -> client.addElementToTailAsync(listKey,
                                                                  Collections.singletonList("item2".getBytes()),
                                                                  intruderId).get());
    }

    // --- SECTION 2: WRITE LOCK PROTECTION ---

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write")
    void testWriteLockProtection() throws Exception {
        client.lockObjectAsync(testKey, LockType.WRITE_LOCK, ownerId, 30).get();

        // 1. Shared Read (Intruder) - SHOULD SUCCEED
        byte[] data = client.getValueAsync(testKey, intruderId).get();
        assertNotNull(data);

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.updateKeyAsync(testKey, "fail".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD SUCCEED
        byte[] ownerUpdate = client.updateKeyAsync(testKey, "success".getBytes(), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    // --- SECTION 3: READ LOCK PROTECTION ---

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads")
    void testReadLockProtection() throws Exception {
        client.lockObjectAsync(testKey, LockType.READ_LOCK, ownerId, 30).get();

        // 1. Intruder Read - SHOULD SUCCEED
        assertNotNull(client.getValueAsync(testKey, intruderId).get());

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.updateKeyAsync(testKey, "bad".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD ALSO FAIL (Read locks block all mutations)
        assertPermissionDenied(() -> client.updateKeyAsync(testKey, "bad".getBytes(), ownerId).get());
    }

    // --- SECTION 4: LOCK COMPATIBILITY ---

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists")
    void testLockCompatibility() throws Exception {
        // Client A has READ
        client.lockObjectAsync(testKey, LockType.READ_LOCK, ownerId, 30).get();

        // Client B tries WRITE
        LockResponse res = client.lockObjectAsync(testKey, LockType.WRITE_LOCK, intruderId, 30).get();
        assertEquals(LockStatus.CANT_LOCK, res.getResult());

        // Client B tries READ - SHOULD SUCCEED (Shared Read)
        LockResponse resRead = client.lockObjectAsync(testKey, LockType.READ_LOCK, intruderId, 30).get();
        assertEquals(LockStatus.OK, resRead.getResult());
    }

    // --- UTILITIES ---

    private void assertPermissionDenied(Executable runnable) {
        ExecutionException e = assertThrows(ExecutionException.class, runnable);
        StatusRuntimeException grpcEx = (StatusRuntimeException) e.getCause();
        assertEquals(Status.Code.PERMISSION_DENIED,
                     grpcEx.getStatus().getCode(),
                     "Expected PERMISSION_DENIED but got " + grpcEx.getStatus().getCode());
    }

}