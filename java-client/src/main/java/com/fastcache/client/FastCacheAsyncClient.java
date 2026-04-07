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
import com.fastcache.grpc.KeyHintResponse;
import com.fastcache.grpc.KeyPositionRequest;
import com.fastcache.grpc.KeyRangeRequest;
import com.fastcache.grpc.LockRequest;
import com.fastcache.grpc.LockResponse;
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
import java.util.concurrent.TimeUnit;

public class FastCacheAsyncClient {

    private final FastCacheGrpcServiceGrpc.FastCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;
    private final Duration timeout;

    public FastCacheAsyncClient(String host, int port, int defaultClientId, Duration timeout) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .directExecutor().usePlaintext()
                .build();
        this.asyncStub = FastCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.timeout = timeout;
    }
    public FastCacheAsyncClient(String host, int port,int clientId) {
        this(host, port, clientId,Duration.ofSeconds(1));
    }

    public FastCacheAsyncClient(String host, int port) {
        this(host, port, 0,Duration.ofSeconds(1));
    }

    // --- INTERNAL HELPERS ---

    private Key buildKey(String key, KeyHint hint, Integer clientId) {
        int cid = (clientId != null)
                  ? clientId
                  : defaultClientId;
        return (hint == null)
               ? KeyUtils.createKey(key, cid)
               : KeyUtils.createKey(key, hint, cid);
    }

    private GetRequest buildGetReq(String key, KeyHint hint, Integer clientId) {
        return GetRequest.newBuilder().setKey(buildKey(key, hint, clientId)).build();
    }

    private TtlRequest buildTtlReq(String key, KeyHint hint, Integer clientId, Long ttl) {
        TtlRequest.Builder b = TtlRequest.newBuilder().setKey(buildKey(key, hint, clientId));
        if (ttl != null) {
            b.setTtl(ttl);
        }
        return b.build();
    }

    public CompletableFuture<BoolResponse> setTtlAsync(String key, long ttlSeconds, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        TtlRequest request = TtlRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setTtl(ttlSeconds)
                .build();
        asyncStub.withDeadlineAfter(timeout).setTtl(request, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<TtlResponse> getTtlAsync(String key, int clientId) {
        CompletableFuture<TtlResponse> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build();
        asyncStub.withDeadlineAfter(timeout).getTtl(ttlRequest, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<TtlResponse> getTtlAsync(String key) {
        return getTtlAsync(key, defaultClientId);
    }

    // --- SECTION 1: Standard Key-Value (Unary) ---
    public CompletableFuture<byte[]> getAndDeleteValueAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build();
        asyncStub.withDeadlineAfter(timeout).getAndDeleteValue(request, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndDeleteValueAsync(String key) {
        return getAndDeleteValueAsync(key, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createKeyAsync(String key, byte[] value, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateRequest req = CreateRequest.newBuilder()
                .setKey(buildKey(key, null, clientId))
                .setValue(CompressionUtils.compressIfNeeded(value))
                .build();
        asyncStub.withDeadlineAfter(timeout).createKeyValue(req, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<KeyHintResponse> createKeyAsync(String key, byte[] value) {
        return createKeyAsync(key, value, defaultClientId);
    }

    public CompletableFuture<byte[]> getValueAsync(String key, KeyHint hint, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getValue(buildGetReq(key, hint, clientId), new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getValueAsync(String key, KeyHint hint) {
        return getValueAsync(key, hint, defaultClientId);
    }

    public CompletableFuture<byte[]> getValueAsync(String key, int clientId) {
        return getValueAsync(key, null, clientId);
    }

    public CompletableFuture<byte[]> getValueAsync(String key) {
        return getValueAsync(key, null, defaultClientId);
    }

    public CompletableFuture<byte[]> updateKeyAsync(String key, KeyHint hint, byte[] value, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        UpdateRequest req = UpdateRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setValue(CompressionUtils.compressIfNeeded(value))
                .build();
        asyncStub.withDeadlineAfter(timeout).updateValue(req, new DecompressingObserver.Update(future));
        return future;
    }

    public CompletableFuture<byte[]> updateKeyAsync(String key, KeyHint hint, byte[] value) {
        return updateKeyAsync(key, hint, value, defaultClientId);
    }

    public CompletableFuture<byte[]> updateKeyAsync(String key, byte[] value, int clientId) {
        return updateKeyAsync(key, null, value, clientId);
    }

    public CompletableFuture<byte[]> updateKeyAsync(String key, byte[] value) {
        return updateKeyAsync(key, null, value, defaultClientId);
    }

    public CompletableFuture<Boolean> existAsync(String key, KeyHint hint, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).existKey(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> existAsync(String key, KeyHint hint) {
        return existAsync(key, hint, defaultClientId);
    }

    public CompletableFuture<Boolean> existAsync(String key, int clientId) {
        return existAsync(key, null, clientId);
    }

    public CompletableFuture<Boolean> existAsync(String key) {
        return existAsync(key, null, defaultClientId);
    }

    public CompletableFuture<Boolean> removeAsync(String key, KeyHint hint, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).remove(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> removeAsync(String key, KeyHint hint) {
        return removeAsync(key, hint, defaultClientId);
    }

    public CompletableFuture<Boolean> removeAsync(String key, int clientId) {
        return removeAsync(key, null, clientId);
    }

    public CompletableFuture<Boolean> removeAsync(String key) {
        return removeAsync(key, null, defaultClientId);
    }

    // --- SECTION 2: TTL ---

    public CompletableFuture<Boolean> setTtlAsync(String key, KeyHint hint, long ttl, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).setTtl(buildTtlReq(key, hint, clientId, ttl), new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> setTtlAsync(String key, long ttl) {
        return setTtlAsync(key, null, ttl, defaultClientId);
    }

    // --- SECTION 3: Collections (Unary) ---

    public CompletableFuture<KeyHintResponse> createQueueAsync(String key, List<byte[]> initialValue, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateQueueRequest.Builder builder = CreateQueueRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        asyncStub.withDeadlineAfter(timeout).createQueue(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<KeyHintResponse> createQueueAsync(String key, List<byte[]> initialValue) {
        return createQueueAsync(key, initialValue, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createQueueAsync(String key) {
        return createQueueAsync(key, null, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createListAsync(String key, List<byte[]> initialValue, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        asyncStub.withDeadlineAfter(timeout).createList(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<KeyHintResponse> createListAsync(String key, List<byte[]>  initialValue) {
        return createListAsync(key, initialValue, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createVectorAsync(String key, List<byte[]>  initialValue, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setAsArray(true);
        initialValue.forEach(elem -> builder.addValue(CompressionUtils.compressIfNeeded(elem)));
        asyncStub.withDeadlineAfter(timeout).createList(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveFrontAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getAndRemoveFront(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                                    new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveFrontAsync(String key) {
        return getAndRemoveFrontAsync(key, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createVectorAsync(String key, List<byte[]> initialValue) {
        return createVectorAsync(key, initialValue, defaultClientId);
    }

    public CompletableFuture<byte[]> getFrontAsync(String key, KeyHint hint, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getHead(buildGetReq(key, hint, clientId), new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getFrontAsync(String key) {
        return getFrontAsync(key, null, defaultClientId);
    }

    public CompletableFuture<Boolean> addElementToTailAsync(String key,
                                                                 KeyHint hint,
                                                                 List<byte[]> data,
                                                                 int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder b = AddToRequest.newBuilder().setKey(buildKey(key, hint, clientId));
        data.stream().map(CompressionUtils::compressIfNeeded).forEach(b::addValue);
        asyncStub.withDeadlineAfter(timeout).addElementToTail(b.build(), new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToTailAsync(String key, List<byte[]> data) {
        return addElementToTailAsync(key, null, data, defaultClientId);
    }

    public CompletableFuture<byte[]> getElementAtPositionAsync(String key, KeyHint hint, int pos, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setPos(pos)
                .build();
        asyncStub.withDeadlineAfter(timeout).getElementAtPosition(req, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getElementAtPositionAsync(String key, int pos) {
        return getElementAtPositionAsync(key, null, pos, defaultClientId);
    }

    // --- SECTION 4: Collections (Streaming) ---

    public CompletableFuture<List<byte[]>> streamList(String key,
                           KeyHint hint,
                           int clientId) {
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getList(buildGetReq(key, hint, clientId), new StreamBatchObserver(future));
        return future;
    }

    public CompletableFuture<List<byte[]>> streamList(String key,int clientId) {
        return streamList(key, null, clientId);
    }
    public CompletableFuture<List<byte[]>> streamList(String key) {
        return streamList(key, null, defaultClientId);
    }

    public CompletableFuture<LockResponse> lockObjectAsync(String key, LockType type, int clientId, int duration) {
        CompletableFuture<LockResponse> future = new CompletableFuture<>();
        LockRequest req = LockRequest.newBuilder()
                .setKey(buildKey(key, null, clientId))
                .setLockType(type)
                .setClientId(clientId)
                .setLockDuration(duration)
                .build();
        asyncStub.withDeadlineAfter(timeout).lockObject(req, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<UnlockResponse> unLockObjectAsync(String key, int clientId) {
        CompletableFuture<UnlockResponse> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, null, clientId))
                .setClientId(clientId)
                .build();
        asyncStub.withDeadlineAfter(timeout).unlockObject(req, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<UnlockResponse> unLockObjectAsync(String key) {
        return unLockObjectAsync(key, defaultClientId);
    }

    public CompletableFuture<List<byte[]>> streamElementInRange(String key,
                                     boolean isArray,
                                     int start,
                                     int end,
                                     int clientId) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(start)
                .setEnd(end)
                .build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        if (isArray) {
            asyncStub.withDeadlineAfter(timeout).getElementInRangeVector(request, new StreamBatchObserver(future));
        } else {
            asyncStub.withDeadlineAfter(timeout).getElementInRange(request, new StreamBatchObserver(future));
        }
        return future;
    }

    public CompletableFuture<List<byte[]>> streamElementInRange(String key,
                                     boolean isArray,
                                     int start,
                                     int end) {
        return streamElementInRange(key, isArray, start, end, defaultClientId);
    }

    public CompletableFuture<List<byte[]>> streamVector(String key,
                             int clientId) {
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build();
        CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getVector(request, new StreamBatchObserver(future));
        return future;
    }

    public CompletableFuture<List<byte[]>> streamVector(String key) {
        return streamVector(key, defaultClientId);
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPositionAsync(String key, int pos, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(pos)
                .build();
        asyncStub.withDeadlineAfter(timeout).getAndRemoveElementAtPosition(request, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPositionAsync(String key, int pos) {
        return getAndRemoveElementAtPositionAsync(key, pos, defaultClientId);
    }

    // --- SECTION 6: Collection Modification ---

    public CompletableFuture<Boolean> addElementToTailAsync(String key, List<byte[]> data, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        AddToRequest request = builder.build();
        asyncStub.withDeadlineAfter(timeout).addElementToTail(request, new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToHeadAsync(String key, List<byte[]> data, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        AddToRequest request = builder.build();
        asyncStub.withDeadlineAfter(timeout).addElementToHead(request, new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToHeadAsync(String key, List<byte[]> data) {
        return addElementToHeadAsync(key, data, defaultClientId);
    }

    public CompletableFuture<Boolean> addElementToPositionAsync(String key, List<byte[]> data, int pos, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(pos);

        data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        asyncStub.withDeadlineAfter(timeout).addElementToPosition(builder.build(), new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> addElementToPositionAsync(String key, List<byte[]> data, int pos) {
        return addElementToPositionAsync(key, data, pos, defaultClientId);
    }

    public CompletableFuture<Boolean> removeTailAsync(String key, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).removeTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                             new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> removeTailAsync(String key) {
        return removeTailAsync(key, defaultClientId);
    }

    public CompletableFuture<Boolean> removeHeadAsync(String key, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).removeHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                             new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    public CompletableFuture<Boolean> removeHeadAsync(String key) {
        return removeHeadAsync(key, defaultClientId);
    }


    public CompletableFuture<Boolean> removeElementAtPositionAsync(String key, int pos) {
        return removeElementAtPositionAsync(key, pos, defaultClientId);
    }

    public CompletableFuture<Boolean> removeElementAtPositionAsync(String key, int pos, int clientId) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(pos)
                .build();
        asyncStub.withDeadlineAfter(timeout).removeElementAtPosition(request, new CompletableFutureObserver<>(future,BoolResponse::getValue));
        return future;
    }

    // --- LIFECYCLE ---
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public CompletableFuture<byte[]> getFrontAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                          new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getHeadAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                          new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getTailAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.withDeadlineAfter(timeout).getTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                          new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getHeadAsync(String key) {
        return getHeadAsync(key, defaultClientId);
    }

    public CompletableFuture<byte[]> getTailAsync(String key) {
        return getTailAsync(key, defaultClientId);
    }

}