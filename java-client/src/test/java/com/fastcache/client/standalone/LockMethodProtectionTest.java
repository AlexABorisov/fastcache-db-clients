package com.fastcache.client.standalone;

import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LockMethodProtectionTest extends TestBase {

    private final String testKey = "lock_protected_item";
    private final int ownerId = 1;
    private final int intruderId = 2;

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others")
    void testGlobalLockUnaryProtection() throws Exception {
        try {
            client.remove(testKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }

        // Ensure object exists
        client.createKeyValue(testKey, "initial_value".getBytes(StandardCharsets.UTF_8),ownerId).get();

        // Owner locks GLOBAL
        client.lockObject(testKey, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getValue
        assertPermissionDenied(() -> client.getValue(testKey, intruderId).get());

        // 2. Intruder tries updateValue
        assertPermissionDenied(() -> client.updateKeyValue(testKey, "new".getBytes(), intruderId).get());

        // 3. Intruder tries remove
        assertPermissionDenied(() -> client.remove(testKey, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others")
    void testGlobalLockCollectionProtection() throws Exception {
        String listKey = "global_list";
        try {
            client.remove(listKey,0xFFFFFFFF).get();
        }catch ( Exception e){
            //ignore
        }

        client.createList(listKey, List.of("item1".getBytes()),ownerId).get();
        client.lockObject(listKey, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getFront
        assertPermissionDenied(() -> client.getFront(listKey, intruderId).get());

        // 2. Intruder tries addElementToTail
        assertPermissionDenied(() -> client.addElementToTail(listKey,
                                                                  Collections.singletonList("item2".getBytes()),
                                                                  intruderId).get());
    }

    // --- SECTION 2: WRITE LOCK PROTECTION ---

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write")
    void testWriteLockProtection() throws Exception {
        try {
            client.remove(testKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(testKey, "initial_value".getBytes(StandardCharsets.UTF_8),ownerId).get();

        client.lockObject(testKey, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Shared Read (Intruder) - SHOULD SUCCEED
        byte[] data = client.getValue(testKey, intruderId).get();
        assertNotNull(data);

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.updateKeyValue(testKey, "fail".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD SUCCEED
        byte[] ownerUpdate = client.updateKeyValue(testKey, "success".getBytes(), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    // --- SECTION 3: READ LOCK PROTECTION ---

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads")
    void testReadLockProtection() throws Exception {
        try {
            client.remove(testKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(testKey, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        client.lockObject(testKey, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder Read - SHOULD SUCCEED
        assertNotNull(client.getValue(testKey, intruderId).get());

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.updateKeyValue(testKey, "bad".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD ALSO FAIL (Read locks block all mutations)
        assertPermissionDenied(() -> client.updateKeyValue(testKey, "bad".getBytes(), ownerId).get());
    }

    // --- SECTION 4: LOCK COMPATIBILITY ---

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists")
    void testLockCompatibility() throws Exception {
        try {
            client.remove(testKey,0xFFFFFFFF).get();
        }catch (Exception e){
            //ignore
        }
        client.createKeyValue(testKey, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        // Client A has READ
        client.lockObject(testKey, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // Client B tries WRITE
        LockStatus res = client.lockObject(testKey, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        // Client B tries READ - SHOULD SUCCEED (Shared Read)
        LockStatus resRead = client.lockObject(testKey, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, resRead);
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