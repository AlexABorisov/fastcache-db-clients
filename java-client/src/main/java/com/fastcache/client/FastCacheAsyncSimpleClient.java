package com.fastcache.client;

import com.fastcache.client.intercece.FastCacheClientInterface;
import com.fastcache.grpc.AddToRequest;
import com.fastcache.grpc.BoolResponse;
import com.fastcache.grpc.CreateListRequest;
import com.fastcache.grpc.CreateQueueRequest;
import com.fastcache.grpc.CreateRequest;
import com.fastcache.grpc.FastCacheGrpcServiceGrpc;
import com.fastcache.grpc.GetRequest;
import com.fastcache.grpc.Key;
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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FastCacheAsyncSimpleClient implements FastCacheClientInterface {

    private final FastCacheGrpcServiceGrpc.FastCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final String target;

    public FastCacheAsyncSimpleClient(String host, int port, int defaultClientId, Duration timeout) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).directExecutor().usePlaintext().build();
        this.asyncStub = FastCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        target = host+":"+port;
    }


    public FastCacheAsyncSimpleClient(String host, int port, int clientId) {
        this(host, port, clientId, Duration.ofSeconds(1));
    }

    public FastCacheAsyncSimpleClient(String host, int port) {
        this(host, port, 0, Duration.ofSeconds(1));
    }

    public FastCacheAsyncSimpleClient(String host, int port, Duration duration) {
        this(host, port, 0, duration);
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel) {
        this(channel,0 );
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel, int clientId) {
        this(channel, clientId, Duration.ofSeconds(1));
    }

    public FastCacheAsyncSimpleClient(ManagedChannel channel, int defaultClientId, Duration duration) {
        this.channel = channel;
        this.asyncStub = FastCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = duration;
        this.target = channel.toString();
    }

    @Override
    public String getTarget() {
        return target;
    }

    @Override
    public int getDefaultClientId() {
        return defaultClientId;
    }

    @Override
    public Duration getDefaultTimeout() {
        return defaultTimeout;
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
    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        TtlRequest request = TtlRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .setTtl(ttl)
                .build();
        getStub(timeout).setTtl(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
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
    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder()
                .setKey(hint == null
                        ? KeyUtils.createKey(key, clientId)
                        : KeyUtils.createKey(key, hint, clientId))
                .build();
        getStub(timeout).getTtl(ttlRequest, new CompletableFutureObserver<>(future, TtlResponse::getTtl));
        return future;
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
    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId))
                .build();
        getStub(timeout).getAndDeleteValue(request, new DecompressingObserver(future));
        return future;
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
    @Override
    public CompletableFuture<KeyHint> createKeyValue(byte[] key,KeyHint hint, byte[] value, int clientId, Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(KeyUtils.createKey(key,getKeyHint(key,hint), clientId))
                .setValue(CompressionUtils.compressIfNeeded(value))
                .build();
        getStub(timeout).createKeyValue(req, new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
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
    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getValue(buildGetReq(key, getKeyHint(key,hint), clientId), new DecompressingObserver(future));
        return future;
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
    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHint hint,
                                                    byte[] value,
                                                    int clientId,
                                                    Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        UpdateRequest req = UpdateRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key,hint), clientId))
                .setValue(CompressionUtils.compressIfNeeded(value))
                .build();
        getStub(timeout).updateValue(req, new DecompressingObserver.Update(future));
        return future;
    }

    /**
     * Checks if key is exist in storage
     *
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).existKey(buildGetReq(key, getKeyHint(key,hint), clientId),
                                                new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Removes exact key
     *
     * @param hint     key hint with strong and weak hashes
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout)
                .remove(buildGetReq(key, getKeyHint(key,hint), clientId),
                        new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    /**
     * Creates queue with initial values
     *
     * @param key      key
     * @param clientId client id 0 - default client id
     * @param timeout  call timeout
     * @return CompletableFuture with response
     */
    @Override
    public CompletableFuture<KeyHint> createQueue(byte[] key,
                                                  List<byte[]> initialValue,
                                                  int clientId,
                                                  Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateQueueRequest.Builder builder = CreateQueueRequest.newBuilder().setKey(KeyUtils.createKey(key,getKeyHint(key), clientId));
        if (initialValue != null) {
            initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        }
        getStub(timeout).createQueue(builder.build(), new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    @Override
    public CompletableFuture<KeyHint> createList(byte[] key,
                                                 List<byte[]> initialValue,
                                                 int clientId,
                                                 Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder()
                .setKey(KeyUtils.createKey(key,getKeyHint(key), clientId))
                .setAsArray(false);
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        getStub(timeout).createList(builder.build(), new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    @Override
    public CompletableFuture<KeyHint> createVector(byte[] key,
                                                   List<byte[]> initialValue,
                                                   int clientId,
                                                   Duration timeout) {
        CompletableFuture<KeyHint> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder()
                .setKey(KeyUtils.createKey(key,getKeyHint(key), clientId))
                .setAsArray(true);
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        getStub(timeout).createList(builder.build(), new CompletableFutureObserver<>(future, KeyHintResponse::getKeyHint));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getAndRemoveFront(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId)).build(),
                                                         new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getHead(buildGetReq(key, getKeyHint(key,hint), clientId), new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder b = AddToRequest.newBuilder().setKey(buildKey(key, getKeyHint(key,hint), clientId));
        data.stream().map(CompressionUtils::compressIfNeeded).forEach(b::addValue);
        getStub(timeout).addElementToTail(b.build(), new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getElementAtPosition(byte[] key,
                                                          KeyHint hint,
                                                          int pos,
                                                          int clientId,
                                                          Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key,hint), clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getElementAtPosition(req, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        getStub(timeout).getList(buildGetReq(key, getKeyHint(key,hint), clientId), new StreamBatchObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key,
                                                    KeyHint hint,
                                                    LockType type,
                                                    int clientId,
                                                    Duration duration,
                                                    Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        LockRequest req = LockRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key,hint), clientId))
                .setLockType(type)
                .setClientId(clientId)
                .setLockDuration((int) duration.toSeconds())
                .build();
        getStub(timeout).lockObject(req, new CompletableFutureObserver<>(future, LockResponse::getResult));
        return future;
    }

    @Override
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<LockStatus> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, getKeyHint(key,hint), clientId))
                .setClientId(clientId)
                .build();
        getStub(timeout).unlockObject(req, new CompletableFutureObserver<>(future, UnlockResponse::getResult));
        return future;
    }

    @Override
    public CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                                KeyHint hint,
                                                                boolean isArray,
                                                                int start,
                                                                int end,
                                                                int clientId,
                                                                Duration timeout) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId))
                .setPos(start)
                .setEnd(end)
                .build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();

        if (isArray) {
            getStub(timeout).getElementInRangeVector(request, new StreamBatchObserver(future));
        } else {
            getStub(timeout).getElementInRange(request, new StreamBatchObserver(future));
        }
        return future;
    }

    @Override
    public CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId)).build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        getStub(timeout).getVector(request, new StreamBatchObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key,
                                                                   KeyHint hint,
                                                                   int pos,
                                                                   int clientId,
                                                                   Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId))
                .setPos(pos)
                .build();
        getStub(timeout).getAndRemoveElementAtPosition(request, new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId));
        if (data != null) {
            data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        }
        AddToRequest request = builder.build();
        getStub(timeout).addElementToHead(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> addElementToPosition(byte[] key,
                                                           KeyHint hint,
                                                           List<byte[]> data,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setPos(pos).setKey(buildKey(key, getKeyHint(key,hint), clientId));
        if (data != null) {
            data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        }
        getStub(timeout).addElementToPosition(builder.build(),
                                                            new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId)).build(),
                                                  new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        getStub(timeout).removeHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId)).build(),
                                                  new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                              KeyHint hint,
                                                              int pos,
                                                              int clientId,
                                                              Duration timeout) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId))
                .setPos(pos)
                .build();
        getStub(timeout).removeElementAtPosition(request, new CompletableFutureObserver<>(future, BoolResponse::getValue));
        return future;
    }

    // --- LIFECYCLE ---
    @Override
    public void shutdown() {
        channel.shutdown();
    }

    @Override
    public CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId)).build(),
                                               new DecompressingObserver(future));
        return future;
    }

    @Override
    public CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        getStub(timeout).getTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, getKeyHint(key,hint), clientId)).build(),
                                               new DecompressingObserver(future));
        return future;
    }

    private Key buildKey(byte[] key, KeyHint hint, int clientId) {
        int cid = (clientId != 0) ? clientId : defaultClientId;
        return (hint == null)
               ? KeyUtils.createKey(key, cid)
               : KeyUtils.createKey(key, hint, cid);
    }

    private GetRequest buildGetReq(byte[] key, KeyHint hint, Integer clientId) {
        return GetRequest.newBuilder().setKey(buildKey(key, getKeyHint(key,hint), clientId)).build();
    }

    private FastCacheGrpcServiceGrpc.FastCacheGrpcServiceStub getStub(Duration timeout) {
        if (timeout != null ) {
            return asyncStub.withDeadlineAfter(timeout);
        }
        return asyncStub;
    }
}