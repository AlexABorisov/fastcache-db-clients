package com.fastcache.client;

import com.fastcache.grpc.AddToRequest;
import com.fastcache.grpc.BoolResponse;
import com.fastcache.grpc.CreateListRequest;
import com.fastcache.grpc.CreateQueueRequest;
import com.fastcache.grpc.CreateRequest;
import com.fastcache.grpc.FastCacheGrpcServiceGrpc;
import com.fastcache.grpc.GetRequest;
import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.KeyHintResponse;
import com.fastcache.grpc.KeyPositionRequest;
import com.fastcache.grpc.LockRequest;
import com.fastcache.grpc.LockResponse;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import com.fastcache.grpc.TtlRequest;
import com.fastcache.grpc.TtlResponse;
import com.fastcache.grpc.UnLockRequest;
import com.fastcache.grpc.UnlockResponse;
import com.fastcache.grpc.UpdateRequest;
import com.fastcache.utils.CompletableFutureObserver;
import com.fastcache.utils.CompressionUtils;
import com.fastcache.utils.DecompressingObserver;
import com.fastcache.utils.StreamBatchObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FastCacheAsyncClient {

    private final FastCacheGrpcServiceGrpc.FastCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration defaultTimeout;

    public FastCacheAsyncClient(String host, int port, int defaultClientId, Duration timeout) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();
        this.asyncStub = FastCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
    }

    public FastCacheAsyncClient(String host, int port, int clientId) {
        this(host, port, clientId, Duration.ofSeconds(1));
    }

    public FastCacheAsyncClient(String host, int port) {
        this(host, port, 0, Duration.ofSeconds(1));
    }

    public FastCacheAsyncClient(String host, int port,Duration duration) {
        this(host, port, 0, duration);
    }

    public FastCacheAsyncClient(ManagedChannel channel) {
        this(channel, 0);
    }

    public FastCacheAsyncClient(ManagedChannel channel, int defaultClientId) {
        this.channel = channel;
        this.asyncStub = FastCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = Duration.ofSeconds(1);
    }

    private Key buildKey(byte[] key, KeyHint hint, Integer clientId) {
        int cid = (clientId != null)
                  ? clientId
                  : defaultClientId;
        return (hint == null)
               ? KeyUtils.createKey(key, cid)
               : KeyUtils.createKey(key, hint, cid);
    }

    private GetRequest buildGetReq(byte[] key, KeyHint hint, Integer clientId) {
        return GetRequest.newBuilder().setKey(buildKey(key, hint, clientId)).build();
    }

    /**
     * Set TTL for key
     *
     * @param key key - must exist
     * @param ttl time to leave in milliseconds
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> setTtl(String key, long ttl) {
        return setTtl(key.getBytes(StandardCharsets.UTF_8), null, ttl, defaultClientId, defaultTimeout);
    }

    /**
     * Set TTL for key
     *
     * @param key      key - must exist
     * @param ttl      time to leave in milliseconds
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> setTtl(String key, long ttl, int clientId) {
        return setTtl(key.getBytes(StandardCharsets.UTF_8), null, ttl, clientId, defaultTimeout);
    }

    /**
     * Set TTL for key
     *
     * @param key      key - must exist
     * @param ttl      time to leave in milliseconds
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> setTtl(String key, KeyHint hint, long ttl, int clientId) {
        return setTtl(key.getBytes(StandardCharsets.UTF_8), hint, ttl, clientId, defaultTimeout);
    }

    /**
     * Set TTL for key
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param ttl      time to leave in milliseconds
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        TtlRequest request = TtlRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .setTtl(ttl)
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.setTtl(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Get TTL for existing key
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getTtl(ttlRequest, new CompletableFutureObserver<>(future, TtlResponse::getTtl));
        return future;
    }

    /**
     * Get TTL for existing key
     *
     * @param key      key - must exist
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<Long> getTtl(String key, int clientId) {
        return getTtl(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    /**
     * Get TTL for existing key
     *
     * @param key  key - must exist
     * @param hint key hint with strong and weak hashes
     * @return CompletableFuture with response
     */
    public CompletableFuture<Long> getTtl(String key, KeyHint hint) {
        return getTtl(key.getBytes(StandardCharsets.UTF_8), hint, defaultClientId, defaultTimeout);
    }

    /**
     * Get TTL for existing key
     *
     * @param key key - must exist
     * @return CompletableFuture with response
     */
    public CompletableFuture<Long> getTtl(String key) {
        return getTtl(key, defaultClientId);
    }

    /**
     * Get and delete key from database
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getAndDeleteValue(request, new DecompressingObserver(future));
        return future;
    }

    /**
     * Get and delete key from database
     *
     * @param key      key - must exist
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint, int clientId) {
        return getAndDeleteValue(key.getBytes(StandardCharsets.UTF_8), hint, clientId, defaultTimeout);
    }

    /**
     * Get and delete key from database
     *
     * @param key  key - must exist
     * @param hint key hint with strong and weak hashes
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint) {
        return getAndDeleteValue(key.getBytes(StandardCharsets.UTF_8), hint, defaultClientId, defaultTimeout);
    }

    /**
     * Get and delete key from database
     *
     * @param key      key - must exist
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getAndDeleteValue(String key, int clientId) {
        return getAndDeleteValue(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    /**
     * Get and delete key from database
     *
     * @param key key - must exist
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getAndDeleteValue(String key) {
        return getAndDeleteValue(key, defaultClientId);
    }

    /**
     * Create key value pair
     *
     * @param key      key for value
     * @param value    value
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value, int clientId, Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setValue(CompressionUtils.compressIfNeeded(value))
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(defaultTimeout);
        }
        asyncStub.createKeyValue(req, new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    /**
     * Create key value pair
     *
     * @param key   key for value
     * @param value value
     * @return CompletableFuture with response
     */
    public CompletableFuture<KeyHint> createKeyValue(String key, byte[] value) {
        return createKeyValue(key.getBytes(StandardCharsets.UTF_8), value, defaultClientId, defaultTimeout);
    }
    public CompletableFuture<KeyHint> createKeyValue(String key, byte[] value,int clientId) {
        return createKeyValue(key.getBytes(StandardCharsets.UTF_8), value, clientId, defaultTimeout);
    }

    /**
     * Get value for exact key. Applicable for RAW values only
     *
     * @param key      key for value
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getValue(buildGetReq(key, hint, clientId), new DecompressingObserver(future));
        return future;
    }

    /**
     * Get value for exact key. Applicable for RAW values only
     *
     * @param key  key for value
     * @param hint key hint with strong and weak hashes
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getValue(String key, KeyHint hint) {
        return getValue(key.getBytes(StandardCharsets.UTF_8), hint, defaultClientId, defaultTimeout);
    }

    /**
     * Get value for exact key. Applicable for RAW values only
     *
     * @param key      key for value
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getValue(String key, int clientId) {
        return getValue(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    /**
     * Get value for exact key. Applicable for RAW values only
     *
     * @param key key for value
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> getValue(String key) {
        return getValue(key, null);
    }

    /**
     * Update key. Old value will be in response
     *
     * @param key      key
     * @param hint     key hint with strong and weak hashes
     * @param value    new value
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHint hint,
                                                    byte[] value,
                                                    int clientId,
                                                    Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        UpdateRequest req = UpdateRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setValue(CompressionUtils.compressIfNeeded(value))
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.updateValue(req, new DecompressingObserver.Update(future));
        return future;
    }

    /**
     * Update key. Old value will be in response
     *
     * @param hint  key hint with strong and weak hashes
     * @param value new value
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> updateKeyValue(String key, KeyHint hint, byte[] value) {
        return updateKeyValue(key.getBytes(StandardCharsets.UTF_8), hint, value, defaultClientId, defaultTimeout);
    }

    /**
     * Update key. Old value will be in response
     *
     * @param value    new value
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> updateKeyValue(String key, byte[] value, int clientId) {
        return updateKeyValue(key.getBytes(StandardCharsets.UTF_8), null, value, clientId, defaultTimeout);
    }

    /**
     * Update key. Old value will be in response
     *
     * @param value new value
     * @return CompletableFuture with response
     */
    public CompletableFuture<byte[]> updateKeyValue(String key, byte[] value) {
        return updateKeyValue(key, value, defaultClientId);
    }

    /**
     * Checks if key is exist in storage
     *
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.withDeadlineAfter(defaultTimeout)
                .existKey(buildGetReq(key, hint, clientId),
                          new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Checks if key is exist in storage
     *
     * @param hint key hint with strong and weak hashes
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> existKey(String key, KeyHint hint) {
        return existKey(key.getBytes(StandardCharsets.UTF_8), hint, defaultClientId, defaultTimeout);
    }

    /**
     * Checks if key is exist in storage
     *
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> existKey(String key, int clientId) {
        return existKey(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    /**
     * Checks if key is exist in storage
     *
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> existKey(String key) {
        return existKey(key, null);
    }

    /**
     * Removes exact key
     *
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.withDeadlineAfter(defaultTimeout)
                .remove(buildGetReq(key, hint, clientId),
                        new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Removes exact key
     *
     * @param hint key hint with strong and weak hashes
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> remove(String key, KeyHint hint) {
        return remove(key.getBytes(StandardCharsets.UTF_8), hint, defaultClientId, defaultTimeout);
    }

    /**
     * Removes exact key
     *
     * @param clientId client id 0 - default client id
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> remove(String key, int clientId) {
        return remove(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    /**
     * Removes exact key
     *
     * @return CompletableFuture with response
     */
    public CompletableFuture<Boolean> remove(String key) {
        return remove(key, null);
    }

    /**
     * Creates queue with initial values
     *
     * @param key      key
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    public CompletableFuture<KeyHint> createQueue(byte[] key,
                                                  List<byte[]> initialValue,
                                                  int clientId,
                                                  Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateQueueRequest.Builder builder = CreateQueueRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        if (initialValue != null) {
            initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        }
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.createQueue(builder.build(), new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    /**
     * Creates queue with initial values
     *
     * @param key key
     * @return CompletableFuture with response
     */
    public CompletableFuture<KeyHint> createQueue(String key, List<byte[]> initialValue) {
        return createQueue(key.getBytes(StandardCharsets.UTF_8), initialValue, defaultClientId, defaultTimeout);
    }

    /**
     * Creates queue with initial values
     *
     * @param key key
     * @return CompletableFuture with response
     */
    public CompletableFuture<KeyHint> createQueue(String key) {
        return createQueue(key.getBytes(StandardCharsets.UTF_8), null, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<KeyHint> createList(byte[] key,
                                                 List<byte[]> initialValue,
                                                 int clientId,
                                                 Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setAsArray(false);
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.createList(builder.build(), new CompletableFutureObserver<>(future,KeyHintResponse::getKeyHint));
        return future;
    }

    public CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue) {
        return createList(key.getBytes(StandardCharsets.UTF_8), initialValue, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue, int clientId) {
        return createList(key.getBytes(StandardCharsets.UTF_8), initialValue, clientId, defaultTimeout);
    }

    public CompletableFuture<KeyHint> createVector(byte[] key,
                                                   List<byte[]> initialValue,
                                                   int clientId,
                                                   Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setAsArray(true);
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.createList(builder.build(), new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    public CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue) {
        return createVector(key, initialValue, defaultClientId);
    }

    public CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue, int clientId) {
        return createVector(key.getBytes(StandardCharsets.UTF_8), initialValue, clientId, defaultTimeout);
    }

    public CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getAndRemoveFront(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, hint, clientId)).build(),
                                    new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveFront(String key,int clientId) {
        return getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    public CompletableFuture<byte[]> getAndRemoveFront(String key) {
        return getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8), null, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getHead(buildGetReq(key, hint, clientId), new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getFront(String key) {
        return getFront(key.getBytes(StandardCharsets.UTF_8), null, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<byte[]> getFront(String key,int clientId) {
        return getFront(key.getBytes(StandardCharsets.UTF_8), null, clientId,defaultTimeout);
    }

    public CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHint hint, List<byte[]> data, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder b = AddToRequest.newBuilder().setKey(buildKey(key, hint, clientId));
        data.stream().map(CompressionUtils::compressIfNeeded).forEach(b::addValue);
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .addElementToTail(b.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data) {
        return addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, data, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data,int clientId) {
        return addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, data, clientId,defaultTimeout);
    }
    public CompletableFuture<byte[]> getElementAtPositionAsync(byte[] key, KeyHint hint, int pos, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setPos(pos)
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getElementAtPosition(req, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getElementAtPositionAsync(String key, int pos) {
        return getElementAtPositionAsync(key.getBytes(StandardCharsets.UTF_8), null, pos, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<byte[]> getElementAtPositionAsync(String key, int pos,int clientId) {
        return getElementAtPositionAsync(key.getBytes(StandardCharsets.UTF_8), null, pos, clientId,defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId,Duration timeout) {
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .getList(buildGetReq(key, hint, clientId), new StreamBatchObserver(future));
        return future;
    }

    public CompletableFuture<List<byte[]>> streamList(String key, int clientId) {
        return streamList(key.getBytes(StandardCharsets.UTF_8), null, clientId,defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamList(String key) {
        return streamList(key.getBytes(StandardCharsets.UTF_8), null, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<LockStatus> lockObject(byte[] key,KeyHint hint, LockType type, int clientId, Duration duration, Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        LockRequest req = LockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setLockType(type)
                .setClientId(clientId)
                .setLockDuration((int) duration.toSeconds())
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.lockObject(req, new CompletableFutureObserver<>(future,LockResponse::getResult));
        return future;
    }

    public CompletableFuture<LockStatus> lockObject(byte[] key,KeyHint hint, LockType type, Duration duration){
        return lockObject(key,hint,type,defaultClientId,duration,defaultTimeout);
    }

    public CompletableFuture<LockStatus> lockObject(String key, LockType type,int clientId, Duration duration) {
        return lockObject(key.getBytes(StandardCharsets.UTF_8), null, type, clientId, duration, defaultTimeout);
    }

    public CompletableFuture<LockStatus> lockObject(String key, LockType type, Duration duration) {
        return lockObject(key.getBytes(StandardCharsets.UTF_8), null, type, defaultClientId, duration, defaultTimeout);
    }

    public CompletableFuture<LockStatus> unlockObject(byte[] key,KeyHint hint, int clientId,Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setClientId(clientId)
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.unlockObject(req, new CompletableFutureObserver<>(future,UnlockResponse::getResult));
        return future;
    }

    public CompletableFuture<LockStatus> unlockObject(String key,KeyHint hint, int clientId) {
        return unlockObject(key.getBytes(StandardCharsets.UTF_8),hint, clientId,defaultTimeout);
    }

    public CompletableFuture<LockStatus> unlockObject(String key, int clientId) {
        return unlockObject(key.getBytes(StandardCharsets.UTF_8),null, clientId,defaultTimeout);
    }

    public CompletableFuture<LockStatus> unlockObject(String key) {
        return unlockObject(key.getBytes(StandardCharsets.UTF_8),null, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                                KeyHint hint,
                                                                boolean isArray,
                                                                int start,
                                                                int end,
                                                                int clientId,
                                                                Duration timeout) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key,hint, clientId))
                .setPos(start)
                .setEnd(end)
                .build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        if (isArray) {
            asyncStub
                    .getElementInRangeVector(request, new StreamBatchObserver(future));
        } else {
            asyncStub.getElementInRange(request, new StreamBatchObserver(future));
        }
        return future;
    }

    public CompletableFuture<List<byte[]>> streamElementInRange(String key, boolean isArray, int start, int end) {
        return streamElementInRange(key.getBytes(StandardCharsets.UTF_8),null, isArray, start, end, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamElementInRange(String key, boolean isArray, int start, int end,int clientId) {
        return streamElementInRange(key.getBytes(StandardCharsets.UTF_8),null, isArray, start, end, clientId,defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId,Duration timeout) {
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key,hint, clientId)).build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub.getVector(request, new StreamBatchObserver(future));
        return future;
    }

    public CompletableFuture<List<byte[]>> streamVector(String key) {
        return streamVector(key.getBytes(StandardCharsets.UTF_8),null, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamVector(String key,KeyHint hint) {
        return streamVector(key.getBytes(StandardCharsets.UTF_8),hint, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<List<byte[]>> streamVector(String key, int clientId) {
        return streamVector(key.getBytes(StandardCharsets.UTF_8),null, clientId, defaultTimeout);
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key, KeyHint hint, int pos, int clientId,Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key,hint, clientId))
                .setPos(pos)
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .getAndRemoveElementAtPosition(request, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(String key, int pos) {
        return getAndRemoveElementAtPosition(key.getBytes(StandardCharsets.UTF_8),null, pos, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(String key, int pos,int clientId) {
        return getAndRemoveElementAtPosition(key.getBytes(StandardCharsets.UTF_8),null, pos, clientId,defaultTimeout);
    }
    // --- SECTION 6: Collection Modification ---


    public CompletableFuture<Boolean> addElementToHead(byte[] key, KeyHint hint, List<byte[]> data, int clientId,Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(KeyUtils.createKey(key,hint, clientId));
        if (data != null) data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        AddToRequest request = builder.build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .addElementToHead(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToHead(String key, List<byte[]> data) {
        return addElementToHead(key.getBytes(StandardCharsets.UTF_8), null, data, defaultClientId, defaultTimeout);
    }


    public CompletableFuture<Boolean> addElementToPosition(byte[] key,KeyHint hint, List<byte[]> data, int pos, int clientId,Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setPos(pos).setKey(buildKey(key, hint, clientId));
        if (data != null) data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .addElementToPosition(builder.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos) {
        return addElementToPosition(key.getBytes(StandardCharsets.UTF_8),null, data, pos, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos,int clientId) {
        return addElementToPosition(key.getBytes(StandardCharsets.UTF_8),null, data, pos, clientId,defaultTimeout);
    }

    public CompletableFuture<Boolean> removeTail(byte[] key,KeyHint hint, int clientId,Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .removeTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key,hint, clientId)).build(),
                            new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(key.getBytes(StandardCharsets.UTF_8),null, defaultClientId, defaultTimeout);
    }

    public CompletableFuture<Boolean> removeTail(String key,int clientId) {
        return removeTail(key.getBytes(StandardCharsets.UTF_8),null, clientId, defaultTimeout);
    }

    public CompletableFuture<Boolean> removeHead(byte[] key,KeyHint hint, int clientId,Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .removeHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key,hint, clientId)).build(),
                            new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> removeHead(String key,int clientId) {
        return removeHead(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    public CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(key,defaultClientId);
    }

    public CompletableFuture<Boolean> removeElementAtPositionAsync(String key, int pos) {
        return removeElementAtPositionAsync(key.getBytes(StandardCharsets.UTF_8),null, pos, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<Boolean> removeElementAtPositionAsync(String key, int pos,int clientId) {
        return removeElementAtPositionAsync(key.getBytes(StandardCharsets.UTF_8),null, pos, clientId,defaultTimeout);
    }

    public CompletableFuture<Boolean> removeElementAtPositionAsync(byte[] key,KeyHint hint, int pos, int clientId,Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key,hint, clientId))
                .setPos(pos)
                .build();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .removeElementAtPosition(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // --- LIFECYCLE ---
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }


    public CompletableFuture<byte[]> getHead(byte[] key,KeyHint hint, int clientId,Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .getHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key,hint, clientId)).build(),
                         new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getTailAsync(byte[] key,KeyHint hint, int clientId,Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        if (timeout != null) {
            asyncStub.withDeadlineAfter(timeout);
        }
        asyncStub
                .getTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key,hint, clientId)).build(),
                         new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getHead(String key,int clientId) {
        return getHead(key.getBytes(StandardCharsets.UTF_8),null, clientId,defaultTimeout);
    }

    public CompletableFuture<byte[]> getTailAsync(String key,int clientId) {
        return getTailAsync(key.getBytes(StandardCharsets.UTF_8), null, clientId, defaultTimeout);
    }

    public CompletableFuture<byte[]> getHead(String key) {
        return getHead(key.getBytes(StandardCharsets.UTF_8),null, defaultClientId,defaultTimeout);
    }

    public CompletableFuture<byte[]> getTailAsync(String key) {
        return getTailAsync(key.getBytes(StandardCharsets.UTF_8), null, defaultClientId, defaultTimeout);
    }

}