package com.fastcache.client.cluster.smart;

import com.fastcache.TestBaseCluster;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.grpc.KeyHint;
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
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LockMethodProtectionTest extends TestBaseCluster {

    private final String testKey = "lock_protected_item";
    private final int ownerId = 1;
    private final int intruderId = 2;

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others - Create on Master")
    void testGlobalLockUnaryProtectionCreateOnMaster() throws Exception {
        // Ensure object exists - Create on master
        String testKey1 = testKey + UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Owner locks GLOBAL on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey1, intruderId).get());

        // 2. Intruder tries updateValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1, "new".getBytes(), intruderId).get());

        // 3. Intruder tries remove
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).remove(testKey1, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others - Create on Backup")
    void testGlobalLockUnaryProtectionCreateOnBackup() throws Exception {


        // Ensure object exists - Create on backup
        String testKey1 = testKey + UUID.randomUUID();
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId)
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(500);

        // Owner locks GLOBAL on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey1, intruderId).get());

        // 2. Intruder tries updateValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1, "new".getBytes(), intruderId).get());

        // 3. Intruder tries remove
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).remove(testKey1, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others - Create on Master")
    void testGlobalLockCollectionProtectionCreateOnMaster() throws Exception {
        String listKey = "global_list" + UUID.randomUUID();;

        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createList(listKey, List.of("item1".getBytes()), ownerId).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(listKey, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getFront
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getFront(listKey, intruderId).get());

        // 2. Intruder tries addElementToTail
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(listKey,
                                                             Collections.singletonList("item2".getBytes()),
                                                             intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others - Create on Backup")
    void testGlobalLockCollectionProtectionCreateOnBackup() throws Exception {
        String listKey = "global_list" + UUID.randomUUID();;

        // Create on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createList(listKey, List.of("item1".getBytes()), ownerId).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(listKey, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getFront
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getFront(listKey, intruderId).get());

        // 2. Intruder tries addElementToTail
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(listKey,
                                                             Collections.singletonList("item2".getBytes()),
                                                             intruderId).get());
    }

    // --- SECTION 2: WRITE LOCK PROTECTION ---

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write - Create on Master")
    void testWriteLockProtectionCreateOnMaster() throws Exception {
        // Create on master
        String testKey1 = testKey + UUID.randomUUID();;
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Shared Read (Intruder) - SHOULD SUCCEED
        byte[] data = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey1, intruderId).get();
        assertNotNull(data);

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1, "fail".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD SUCCEED
        byte[] ownerUpdate = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1, "success".getBytes(), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write - Create on Backup")
    void testWriteLockProtectionCreateOnBackup() throws Exception {
        // Create on backup
        String testKey1 = testKey + UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8), ownerId).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Shared Read (Intruder) - SHOULD SUCCEED
        byte[] data = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey1, intruderId).get();
        assertNotNull(data);

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1, "fail".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD SUCCEED
        byte[] ownerUpdate = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1, "success".getBytes(), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    // --- SECTION 3: READ LOCK PROTECTION ---

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads - Create on Master")
    void testReadLockProtectionCreateOnMaster() throws Exception {
        // Create on master
        String testKey1 = testKey + UUID.randomUUID();;
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder Read - SHOULD SUCCEED
        assertNotNull(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey1, intruderId).get());

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1, "bad".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD ALSO FAIL (Read locks block all mutations)
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1, "bad".getBytes(), ownerId).get());
    }

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads - Create on Backup")
    void testReadLockProtectionCreateOnBackup() throws Exception {
        // Create on backup
        String testKey1 = testKey + UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder Read - SHOULD SUCCEED
        assertNotNull(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey1, intruderId).get());

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1, "bad".getBytes(), intruderId).get());

        // 3. Owner Write - SHOULD ALSO FAIL (Read locks block all mutations)
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1, "bad".getBytes(), ownerId).get());
    }

    // --- SECTION 4: LOCK COMPATIBILITY ---

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists - Create on Master")
    void testLockCompatibilityCreateOnMaster() throws Exception {

        // Create on master
        String testKey1 = testKey+ UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Client A has READ on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // Client B tries WRITE
        LockStatus res = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        // Client B tries READ - SHOULD SUCCEED (Shared Read)
        LockStatus resRead = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, resRead);
    }

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists - Create on Backup")
    void testLockCompatibilityCreateOnBackup() throws Exception {
        // Create on backup
        String testKey1 = testKey+ UUID.randomUUID();
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).createKeyValue(testKey1, "initial_value".getBytes(StandardCharsets.UTF_8)).get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Client A has READ on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // Client B tries WRITE
        LockStatus res = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        // Client B tries READ - SHOULD SUCCEED (Shared Read)
        LockStatus resRead = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
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