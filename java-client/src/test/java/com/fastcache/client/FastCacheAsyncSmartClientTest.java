package com.fastcache.client;

import com.fastcache.client.intf.FastCacheClientInterface;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import com.fastcache.grpc.coordinator.NodeRole;
import com.fastcache.utils.Pair;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class FastCacheAsyncSmartClientTest {

    private static final Logger log = LogManager.getLogger(FastCacheAsyncSmartClientTest.class);
    public static final int MAX_SHARDS = 10;

    @Mock
    private FastCacheClientInterface masterClient;

    @Mock
    private FastCacheClientInterface backupClient;

    @Mock
    private FastCacheClientInterface rerouteClient;

    private FastCacheAsyncSmartClient client;
    @Captor
    private ArgumentCaptor<Function<FastCacheClientInterface, CompletableFuture<String>>> actionCaptor;

    @Captor
    private ArgumentCaptor<Function<String, CompletableFuture<String>>> stringActionCaptor;

    private final KeyHint keyHint = KeyHint.newBuilder().setWeekHash(123).build();

    @BeforeEach
    void setUp() {
        client = new FastCacheAsyncSmartClient(mock(ManagedChannel.class),0,Duration.ofSeconds(100));
        client.scheduledExecutorService.shutdownNow();
        client.scheduledExecutorService = Mockito.mock(ScheduledThreadPoolExecutor.class);
        // Set default mode
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        // Initialize static state
        client.routing_info.set(new FastCacheAsyncSmartClient.RoutingInfo(MAX_SHARDS,new ConcurrentHashMap<>(),new ConcurrentHashMap<>()));
        client.readyFlag.set(true);

        // Mock client targets
        when(masterClient.getTarget()).thenReturn("target1");
        when(backupClient.getTarget()).thenReturn("target2");
        when(rerouteClient.getTarget()).thenReturn("reroute_target");


        // Set up routing table
        for(int i= 0; i <MAX_SHARDS ;i++) {
            client.routing_info.get().routingTable().put(Pair.of(NodeRole.MASTER, i),
                                                       masterClient);
            client.routing_info.get().routingTable().put(Pair.of(NodeRole.BACKUP,i),
                                                       backupClient);
        }
        client.routing_info.get().routingTableTarget().put("target1", masterClient);
        client.routing_info.get().routingTableTarget().put("target2", backupClient);
        client.routing_info.get().routingTableTarget().put("reroute_target", rerouteClient);


    }

    @Test
    void testExecuteMasterModeSuccess() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteBackupModeSuccess() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        when(backupClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(backupClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(masterClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterTimeout() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> timeoutFuture = new CompletableFuture<>();
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(timeoutFuture);

        // Simulate timeout
        timeoutFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.DEADLINE_EXCEEDED)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterUnavailableThenBackupSuccess() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> unavailableFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(unavailableFuture);
        when(backupClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // Simulate master unavailable
        unavailableFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterUnavailableThenBackupUnavailable() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> masterFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> backupFuture = new CompletableFuture<>();
        
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(masterFuture);
        when(backupClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(backupFuture);

        // Simulate both unavailable
        masterFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));
        backupFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.UNAVAILABLE, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterReroute() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> rerouteFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(rerouteFuture);
        when(rerouteClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // Create metadata with reroute target
        Metadata trailers = new Metadata();
        trailers.put(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER), "reroute_target");
        
        // Simulate reroute
        rerouteFuture.completeExceptionally(new CompletionException(
            new StatusRuntimeException(Status.FAILED_PRECONDITION, trailers)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(rerouteClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterRerouteThenUnavailable() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> rerouteFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> unavailableFuture = new CompletableFuture<>();
        
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(rerouteFuture);
        when(rerouteClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(unavailableFuture);

        // Create metadata with reroute target
        Metadata trailers = new Metadata();
        trailers.put(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER), "reroute_target");
        
        // Simulate reroute then unavailable
        rerouteFuture.completeExceptionally(new CompletionException(
            new StatusRuntimeException(Status.FAILED_PRECONDITION, trailers)));
        unavailableFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.UNAVAILABLE, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(rerouteClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterTimeoutNoFallback() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> timeoutFuture = new CompletableFuture<>();
        
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(timeoutFuture);

        // Simulate timeout
        timeoutFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.DEADLINE_EXCEEDED)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterNullThenBackupSuccess() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        // Clear master from routing table
        client.routing_info.get().routingTable().remove(Pair.of(NodeRole.MASTER, 3));
        
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        when(backupClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(backupClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(masterClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterNullThenBackupUnavailable() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        // Clear master from routing table
        client.routing_info.get().routingTable().remove(Pair.of(NodeRole.MASTER, 3));
        
        CompletableFuture<byte[]> unavailableFuture = new CompletableFuture<>();
        when(backupClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(unavailableFuture);

        // Simulate backup unavailable
        unavailableFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.UNAVAILABLE, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(backupClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteMasterThanBackupModeSuccess() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    // --- COLLECTION OPERATIONS TESTS ---
    
    @Test
    void testCreateOnMasterValidateOnBackup_createQueue() {
        // Given
        byte[] key = "test_queue".getBytes();
        List<byte[]> initialValue = Arrays.asList("item1".getBytes(), "item2".getBytes());
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<KeyHint> createFuture = CompletableFuture.completedFuture(keyHint);
        when(masterClient.createQueue(any(byte[].class), any(List.class), anyInt(), any(Duration.class))).thenReturn(createFuture);
        
        // When
        CompletableFuture<KeyHint> createResult = client.createQueue(key, initialValue, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertNotNull(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).createQueue(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).createQueue(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<List<byte[]>> validateFuture = CompletableFuture.completedFuture(initialValue);
        when(backupClient.streamList(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(validateFuture);
        
        // When
        CompletableFuture<List<byte[]>> validateResult = client.streamList(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            List<byte[]> result = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(2, result.size());
            assertArrayEquals("item1".getBytes(), result.get(0));
            assertArrayEquals("item2".getBytes(), result.get(1));
        });
        verify(backupClient).streamList(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testCreateOnMasterValidateOnBackup_createList() {
        // Given
        byte[] key = "test_list".getBytes();
        List<byte[]> initialValue = Arrays.asList("item1".getBytes(), "item2".getBytes());
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<KeyHint> createFuture = CompletableFuture.completedFuture(keyHint);
        when(masterClient.createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class))).thenReturn(createFuture);
        
        // When
        CompletableFuture<KeyHint> createResult = client.createList(key, initialValue, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertNotNull(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<List<byte[]>> validateFuture = CompletableFuture.completedFuture(initialValue);
        when(backupClient.streamList(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(validateFuture);
        
        // When
        CompletableFuture<List<byte[]>> validateResult = client.streamList(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            List<byte[]> result = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(2, result.size());
            assertArrayEquals("item1".getBytes(), result.get(0));
            assertArrayEquals("item2".getBytes(), result.get(1));
        });
        verify(backupClient).streamList(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testCreateOnMasterValidateOnBackup_createVector() {
        // Given
        byte[] key = "test_vector".getBytes();
        List<byte[]> initialValue = Arrays.asList("item1".getBytes(), "item2".getBytes());
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<KeyHint> createFuture = CompletableFuture.completedFuture(keyHint);
        when(masterClient.createVector(any(byte[].class), any(List.class), anyInt(), any(Duration.class))).thenReturn(createFuture);
        
        // When
        CompletableFuture<KeyHint> createResult = client.createVector(key, initialValue, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertNotNull(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).createVector(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).createVector(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<List<byte[]>> validateFuture = CompletableFuture.completedFuture(initialValue);
        when(backupClient.streamVector(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(validateFuture);
        
        // When
        CompletableFuture<List<byte[]>> validateResult = client.streamVector(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            List<byte[]> result = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(2, result.size());
            assertArrayEquals("item1".getBytes(), result.get(0));
            assertArrayEquals("item2".getBytes(), result.get(1));
        });
        verify(backupClient).streamVector(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- LOCK OPERATIONS TESTS ---
    
    @Test
    void testCreateOnMasterValidateOnBackup_lockObject() {
        // Given
        byte[] key = "test_key".getBytes();
        
        // Lock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<LockStatus> lockFuture = CompletableFuture.completedFuture(LockStatus.OK);
        when(masterClient.lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class))).thenReturn(lockFuture);
        
        // When
        CompletableFuture<LockStatus> lockResult = client.lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(
                MAX_SHARDS), Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            LockStatus status = lockResult.get(5, TimeUnit.SECONDS);
            assertEquals(LockStatus.OK, status);
        });
        verify(masterClient).lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class));
        verify(backupClient, never()).lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class));
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<LockStatus> validateFuture = CompletableFuture.completedFuture(LockStatus.OK);
        when(backupClient.lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class))).thenReturn(validateFuture);
        
        // When
        CompletableFuture<LockStatus> validateResult = client.lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(
                MAX_SHARDS), Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            LockStatus status = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(LockStatus.OK, status);
        });
        verify(backupClient).lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testCreateOnMasterValidateOnBackup_unlockObject() {
        // Given
        byte[] key = "test_key".getBytes();
        
        // Unlock on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<LockStatus> unlockFuture = CompletableFuture.completedFuture(LockStatus.OK);
        when(masterClient.unlockObject(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(unlockFuture);
        
        // When
        CompletableFuture<LockStatus> unlockResult = client.unlockObject(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            LockStatus status = unlockResult.get(5, TimeUnit.SECONDS);
            assertEquals(LockStatus.OK, status);
        });
        verify(masterClient).unlockObject(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).unlockObject(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<LockStatus> validateFuture = CompletableFuture.completedFuture(LockStatus.CANT_LOCK);
        when(backupClient.lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class))).thenReturn(validateFuture);
        
        // When
        CompletableFuture<LockStatus> validateResult = client.lockObject(key, keyHint, LockType.GLOBAL, 1, Duration.ofSeconds(
                MAX_SHARDS), Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            LockStatus status = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(LockStatus.CANT_LOCK, status);
        });
        verify(backupClient).lockObject(any(byte[].class), any(KeyHint.class), any(LockType.class), anyInt(), any(Duration.class), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- STREAM OPERATIONS TESTS ---
    
    @Test
    void testCreateOnMasterValidateOnBackup_streamList() {
        // Given
        byte[] key = "test_list".getBytes();
        List<byte[]> expectedValues = Arrays.asList("item1".getBytes(), "item2".getBytes());
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<KeyHint> createFuture = CompletableFuture.completedFuture(keyHint);
        when(masterClient.createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class))).thenReturn(createFuture);
        
        // When
        CompletableFuture<KeyHint> createResult = client.createList(key, expectedValues, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertNotNull(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class));

        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<List<byte[]>> validateFuture = CompletableFuture.completedFuture(expectedValues);
        when(backupClient.streamList(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(validateFuture);

        // When
        CompletableFuture<List<byte[]>> validateResult = client.streamList(key, keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            List<byte[]> result = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(2, result.size());
            assertArrayEquals("item1".getBytes(), result.get(0));
            assertArrayEquals("item2".getBytes(), result.get(1));
        });
        verify(backupClient).streamList(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testCreateOnMasterValidateOnBackup_streamVector() {
        // Given
        byte[] key = "test_vector".getBytes();
        List<byte[]> expectedValues = Arrays.asList("item1".getBytes(), "item2".getBytes());
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<KeyHint> createFuture = CompletableFuture.completedFuture(keyHint);
        when(masterClient.createVector(any(), any(), anyInt(), any())).thenReturn(createFuture);
        
        // When
        CompletableFuture<KeyHint> createResult = client.createVector(key, expectedValues, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertNotNull(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).createVector(any(), any(), anyInt(), any());
        verify(backupClient, never()).createVector(any(), any(), anyInt(), any());
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<List<byte[]>> validateFuture = CompletableFuture.completedFuture(expectedValues);
        when(backupClient.streamVector(any(), any(), anyInt(), any())).thenReturn(validateFuture);
        
        // When
        CompletableFuture<List<byte[]>> validateResult = client.streamVector(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            List<byte[]> result = validateResult.get(5, TimeUnit.SECONDS);
            assertEquals(2, result.size());
            assertArrayEquals("item1".getBytes(), result.get(0));
            assertArrayEquals("item2".getBytes(), result.get(1));
        });
        verify(backupClient).streamVector(any(), any(), anyInt(), any());
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testCreateOnMasterValidateOnBackup_streamElementInRange() {
        // Given
        byte[] key = "test_list".getBytes();
        List<byte[]> expectedValues = Arrays.asList("item1".getBytes(), "item2".getBytes(), "item3".getBytes());
        
        // Create on master
        //client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<KeyHint> createFuture = CompletableFuture.completedFuture(keyHint);
        when(masterClient.createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class))).thenReturn(createFuture);

        // When
        CompletableFuture<KeyHint> createResult = client.createList(key, expectedValues, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            assertNotNull(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class));
        verify(backupClient, never()).createList(any(byte[].class), any(List.class), anyInt(), any(Duration.class));

        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<List<byte[]>> validateFuture = CompletableFuture.completedFuture(expectedValues.subList(1, 2));
        when(backupClient.streamElementInRange(any(byte[].class), any(KeyHint.class), anyBoolean(), anyInt(), anyInt(), anyInt(), any(Duration.class))).thenReturn(validateFuture);

        // When
        CompletableFuture<List<byte[]>> validateResult = client.streamElementInRange(key, keyHint, false, 1, 1, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            List<byte[]> result = validateResult.get(5, TimeUnit.SECONDS);
            assertNotNull(result);
            assertEquals(1, result.size());
            assertArrayEquals("item2".getBytes(), result.get(0));
        });
        verify(backupClient).streamElementInRange(any(byte[].class), any(KeyHint.class), anyBoolean(), anyInt(), anyInt(), anyInt(), any(Duration.class));
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testExecuteWithNullHint() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), null, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    // --- ERROR SCENARIOS TESTS ---
    
    @Test
    void testExecuteWithTimeout() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> timeoutFuture = new CompletableFuture<>();
        when(masterClient.getValue(any(), any(), anyInt(), any())).thenReturn(timeoutFuture);

        // Simulate timeout
        timeoutFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.DEADLINE_EXCEEDED)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(masterClient).getValue(any(), any(), anyInt(), any());
        verify(backupClient, never()).getValue(any(), any(), anyInt(), any());
    }

    @Test
    void testExecuteWithNodeFailure() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> failureFuture = new CompletableFuture<>();
        when(masterClient.getValue(any(), any(), anyInt(), any())).thenReturn(failureFuture);

        // Simulate node failure
        failureFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            result.get(5, TimeUnit.SECONDS);
        });
        assertTrue(exception.getCause() instanceof StatusRuntimeException);
        assertEquals(Status.Code.UNAVAILABLE, ((StatusRuntimeException) exception.getCause()).getStatus().getCode());
        verify(masterClient).getValue(any(), any(), anyInt(), any());
        verify(backupClient, never()).getValue(any(), any(), anyInt(), any());
    }

    @Test
    void testExecuteWithRerouting() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> rerouteFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        
        when(masterClient.getValue(any(), any(), anyInt(), any())).thenReturn(rerouteFuture);
        when(rerouteClient.getValue(any(), any(), anyInt(), any())).thenReturn(successFuture);

        // Create metadata with reroute target
        Metadata trailers = new Metadata();
        trailers.put(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER), "reroute_target");
        
        // Simulate reroute
        rerouteFuture.completeExceptionally(new CompletionException(
            new StatusRuntimeException(Status.FAILED_PRECONDITION, trailers)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(), any(), anyInt(), any());
        verify(rerouteClient).getValue(any(), any(), anyInt(), any());
        verify(backupClient, never()).getValue(any(), any(), anyInt(), any());
    }

    @Test
    void testExecuteWithCorrectNodeForwarding() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER_THAN_BACKUP);
        CompletableFuture<byte[]> masterFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> backupFuture = CompletableFuture.completedFuture("success".getBytes());
        
        when(masterClient.getValue(any(), any(), anyInt(), any())).thenReturn(masterFuture);
        when(backupClient.getValue(any(), any(), anyInt(), any())).thenReturn(backupFuture);

        // Simulate master unavailable
        masterFuture.completeExceptionally(new CompletionException(new StatusRuntimeException(Status.UNAVAILABLE)));

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(), any(), anyInt(), any());
        verify(backupClient).getValue(any(), any(), anyInt(), any());
    }

    @Test
    void testExecuteWithRerouteInHandle() {
        // Given
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<byte[]> successFuture = CompletableFuture.completedFuture("success".getBytes());
        when(masterClient.getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class))).thenReturn(successFuture);

        // When
        CompletableFuture<byte[]> result = client.getValue("test_key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // Then
        assertDoesNotThrow(() -> {
            byte[] value = result.get(5, TimeUnit.SECONDS);
            assertArrayEquals("success".getBytes(), value);
        });
        verify(masterClient).getValue(any(byte[].class), any(KeyHint.class), anyInt(), any(Duration.class));
    }

    @Test
    void testExecuteWithRerouteSuccessfullySwitchToRerouteClientG() throws Exception {
        // 1. Given
        byte[] expectedData = "success_from_reroute".getBytes();
        CompletableFuture<byte[]> masterFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> rerouteFuture = new CompletableFuture<>();

        when(masterClient.getValue(any(), any(), anyInt(), any())).thenReturn(masterFuture);
        // You MUST stub the reroute client now because the code WILL call it
        when(rerouteClient.getValue(any(), any(), anyInt(), any())).thenReturn(rerouteFuture);

        // 2. When
        CompletableFuture<byte[]> result = client.getValue("key".getBytes(), keyHint, 1, Duration.ofSeconds(5));

        // 3. Simulate Master Failure with Reroute Metadata
        Metadata trailers = new Metadata();
        trailers.put(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER), "reroute_target");
        masterFuture.completeExceptionally(new StatusRuntimeException(Status.FAILED_PRECONDITION, trailers));

        // 4. Complete the Reroute call
        rerouteFuture.complete(expectedData);

        // 5. Then
        assertArrayEquals(expectedData, result.get(2, TimeUnit.SECONDS));
        verify(masterClient).getValue(any(), any(), anyInt(), any());
        verify(rerouteClient).getValue(any(), any(), anyInt(), any());
    }


    @Test
    void testExecuteShouldRerouteWhenMasterReturnsFailedPrecondition() throws Exception {
        // 1. Given: Define the expected outcome and the "players"
        byte[] key = "test_key".getBytes();
        byte[] expectedData = "data_from_reroute_node".getBytes();

        CompletableFuture<byte[]> masterFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> rerouteFuture = new CompletableFuture<>();

        // Stub Master to fail and Reroute client to succeed
        when(masterClient.getValue(any(), any(), anyInt(), any())).thenReturn(masterFuture);
        when(rerouteClient.getValue(any(), any(), anyInt(), any())).thenReturn(rerouteFuture);

        // 2. When: The user makes a request
        CompletableFuture<byte[]> result = client.getValue(key, keyHint, 1, Duration.ofSeconds(5));

        // 3. Simulate: Master node says "I don't have this, go to 'reroute_target'"
        Metadata trailers = new Metadata();
        trailers.put(
                Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER),
                "reroute_target"
        );

        masterFuture.completeExceptionally(new StatusRuntimeException(Status.FAILED_PRECONDITION, trailers));

        // 4. Simulate: The Reroute node responds successfully
        rerouteFuture.complete(expectedData);

        // 5. Then: The user gets their data without ever seeing the Master failure
        assertArrayEquals(expectedData, result.get(2, TimeUnit.SECONDS));

        // Verify that both clients were touched in the correct order
        verify(masterClient).getValue(any(), any(), anyInt(), any());
        verify(rerouteClient).getValue(any(), any(), anyInt(), any());
    }

    // --- CREATE-ON-MASTER / VALIDATE-ON-BACKUP TESTS ---
    
    @Test
    void testCreateOnMasterValidateOnBackup_setTtl() {
        // Given
        byte[] key = "test_key".getBytes();
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<Boolean> createFuture = CompletableFuture.completedFuture(true);
        when(masterClient.setTtl(any(), any(), anyLong(), anyInt(), any())).thenReturn(createFuture);
        
        // When
        CompletableFuture<Boolean> createResult = client.setTtl(key, keyHint, 1000L, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertTrue(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).setTtl(any(), any(), anyLong(), anyInt(), any());
        verify(backupClient, never()).setTtl(any(), any(), anyLong(), anyInt(), any());
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<Long> validateFuture = CompletableFuture.completedFuture(1000L);
        when(backupClient.getTtl(any(), any(), anyInt(), any())).thenReturn(validateFuture);
        
        // When
        CompletableFuture<Long> validateResult = client.getTtl(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertEquals(1000L, (long) validateResult.get(5, TimeUnit.SECONDS));
        });
        verify(backupClient).getTtl(any(), any(), anyInt(), any());
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testCreateOnMasterValidateOnBackup_getTtl() {
        // Given
        byte[] key = "test_key".getBytes();
        
        // Create on master
        client.setMode(FastCacheAsyncSmartClient.Mode.MASTER);
        CompletableFuture<Boolean> createFuture = CompletableFuture.completedFuture(true);
        when(masterClient.setTtl(any(), any(), anyLong(), anyInt(), any())).thenReturn(createFuture);
        
        // When
        CompletableFuture<Boolean> createResult = client.setTtl(key, keyHint, 1000L, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertTrue(createResult.get(5, TimeUnit.SECONDS));
        });
        verify(masterClient).setTtl(any(), any(), anyLong(), anyInt(), any());
        verify(backupClient, never()).setTtl(any(), any(), anyLong(), anyInt(), any());
        
        // Validate on backup
        client.setMode(FastCacheAsyncSmartClient.Mode.BACKUP);
        CompletableFuture<Long> validateFuture = CompletableFuture.completedFuture(1000L);
        when(backupClient.getTtl(any(), any(), anyInt(), any())).thenReturn(validateFuture);
        
        // When
        CompletableFuture<Long> validateResult = client.getTtl(key, keyHint, 1, Duration.ofSeconds(5));
        
        // Then
        assertDoesNotThrow(() -> {
            assertEquals(1000L, (long) validateResult.get(5, TimeUnit.SECONDS));
        });
        verify(backupClient).getTtl(any(), any(), anyInt(), any());
        
        // Add delay for replication
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}