package com.fastcache.client.cluster.payload;

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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LockMethodProtectionTest extends TestBaseCluster {

    private static final int KEY_SIZE = 1024;
    private static final int VALUE_SIZE = 2048;

    private final int ownerId = 1;
    private final int intruderId = 2;

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others - Create on Master")
    void testGlobalLockUnaryProtectionCreateOnMaster() throws Exception {
        // Ensure object exists - Create on master
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE), ownerId)
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Owner locks GLOBAL on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1,keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey1,keyHint, intruderId).get());

        // 2. Intruder tries updateValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), intruderId).get());

        // 3. Intruder tries remove
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).remove(testKey1,keyHint, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Unary Get/Update/Remove from others - Create on Backup")
    void testGlobalLockUnaryProtectionCreateOnBackup() throws Exception {


        // Ensure object exists - Create on backup
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE), ownerId)
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Owner locks GLOBAL on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1, keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey1,keyHint, intruderId).get());

        // 2. Intruder tries updateValue
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), intruderId).get());

        // 3. Intruder tries remove
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).remove(testKey1,keyHint, intruderId).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others - Create on Master")
    void testGlobalLockCollectionProtectionCreateOnMaster() throws Exception {
        byte[] listKey = createLargePayload(KEY_SIZE);

        // Create on master
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createList(listKey, List.of(createLargePayload(VALUE_SIZE)), ownerId,Duration.of(1, ChronoUnit.SECONDS))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(listKey,keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getFront
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getFront(listKey,keyHint, intruderId,Duration.ofSeconds(30)).get());

        // 2. Intruder tries addElementToTail
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).addElementToTail(listKey,keyHint,
                                                             Collections.singletonList(createLargePayload(VALUE_SIZE)),
                                                             intruderId,Duration.ofSeconds(30)).get());
    }

    @Test
    @DisplayName("GLOBAL Lock: Blocks Collection operations from others - Create on Backup")
    void testGlobalLockCollectionProtectionCreateOnBackup() throws Exception {
        byte[] listKey = createLargePayload(KEY_SIZE);

        // Create on backup
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createList(listKey, List.of(createLargePayload(VALUE_SIZE)), ownerId,Duration.of(1,ChronoUnit.SECONDS))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(listKey,keyHint, LockType.GLOBAL, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder tries getFront
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getFront(listKey,keyHint, intruderId,Duration.of(1,ChronoUnit.SECONDS)).get());

        // 2. Intruder tries addElementToTail
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).addElementToTail(listKey,keyHint,
                                                             Collections.singletonList(createLargePayload(VALUE_SIZE)),
                                                             intruderId,Duration.of(1, ChronoUnit.SECONDS)).get());
    }

    // --- SECTION 2: WRITE LOCK PROTECTION ---

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write - Create on Master")
    void testWriteLockProtectionCreateOnMaster() throws Exception {
        // Create on master
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE), ownerId)
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1,keyHint, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Shared Read (Intruder) - SHOULD SUCCEED
        byte[] data = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey1,keyHint, intruderId).get();
        assertNotNull(data);

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), intruderId).get());

        // 3. Owner Write - SHOULD SUCCEED
        byte[] ownerUpdate = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    @Test
    @DisplayName("WRITE Lock: Allows Shared Read but Blocks Intruder Write - Create on Backup")
    void testWriteLockProtectionCreateOnBackup() throws Exception {
        // Create on backup
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE), ownerId)
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1,keyHint, LockType.WRITE_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Shared Read (Intruder) - SHOULD SUCCEED
        byte[] data = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey1,keyHint, intruderId).get();
        assertNotNull(data);

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), intruderId).get());

        // 3. Owner Write - SHOULD SUCCEED
        byte[] ownerUpdate = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), ownerId).get();
        assertNotNull(ownerUpdate);
    }

    // --- SECTION 3: READ LOCK PROTECTION ---

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads - Create on Master")
    void testReadLockProtectionCreateOnMaster() throws Exception {
        // Create on master
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1,keyHint, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder Read - SHOULD SUCCEED
        assertNotNull(client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).getValue(testKey1,keyHint, intruderId).get());

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), intruderId).get());

        // 3. Owner Write - SHOULD ALSO FAIL (Read locks block all mutations)
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), ownerId).get());
    }

    @Test
    @DisplayName("READ Lock: Blocks all Writes but allows all Reads - Create on Backup")
    void testReadLockProtectionCreateOnBackup() throws Exception {
        // Create on backup
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1,keyHint, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // 1. Intruder Read - SHOULD SUCCEED
        assertNotNull(client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).getValue(testKey1,keyHint, intruderId).get());

        // 2. Intruder Write - SHOULD FAIL
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), intruderId).get());

        // 3. Owner Write - SHOULD ALSO FAIL (Read locks block all mutations)
        assertPermissionDenied(() -> client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).updateKeyValue(testKey1,keyHint, createLargePayload(VALUE_SIZE), ownerId).get());
    }

    // --- SECTION 4: LOCK COMPATIBILITY ---

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists - Create on Master")
    void testLockCompatibilityCreateOnMaster() throws Exception {

        // Create on master
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Client A has READ on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1,keyHint, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // Client B tries WRITE
        LockStatus res = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1,keyHint, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        // Client B tries READ - SHOULD SUCCEED (Shared Read)
        LockStatus resRead = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP).lockObject(testKey1,keyHint, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.OK, resRead);
    }

    @Test
    @DisplayName("Compatibility: Cannot acquire WRITE if READ exists - Create on Backup")
    void testLockCompatibilityCreateOnBackup() throws Exception {
        // Create on backup
        byte[] testKey1 = createLargePayload(KEY_SIZE);
        KeyHint keyHint = client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP)
                .createKeyValue(testKey1, createLargePayload(VALUE_SIZE))
                .get();
        // Allow cache to replicate data inside cluster
        Thread.sleep(150);

        // Client A has READ on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1,keyHint, LockType.READ_LOCK, ownerId, Duration.ofSeconds(30)).get();

        // Client B tries WRITE
        LockStatus res = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1,keyHint, LockType.WRITE_LOCK, intruderId, Duration.ofSeconds(30)).get();
        assertEquals(LockStatus.CANT_LOCK, res);

        // Client B tries READ - SHOULD SUCCEED (Shared Read)
        LockStatus resRead = client.setMode(FastCacheAsyncSmartClient.Mode.MASTER).lockObject(testKey1,keyHint, LockType.READ_LOCK, intruderId, Duration.ofSeconds(30)).get();
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