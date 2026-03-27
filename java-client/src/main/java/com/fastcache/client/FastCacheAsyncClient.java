package com.fastcache.client;

import com.fastcache.grpc.AddToRequest;
import com.fastcache.grpc.BatchValueResponse;
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
import com.fastcache.grpc.UpdateValueResponse;
import com.fastcache.grpc.ValueResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * FastCache High-Performance Async Client for i9 Architectures.
 * Supports 4-tier method overloading:
 * 1. (key, hint, clientId, ...)
 * 2. (key, hint, ...) -> uses defaultClientId
 * 3. (key, clientId, ...) -> no hint
 * 4. (key, ...) -> uses defaultClientId, no hint
 */
public class FastCacheAsyncClient {

    private final FastCacheGrpcServiceGrpc.FastCacheGrpcServiceStub asyncStub;
    private final ManagedChannel channel;
    private final int defaultClientId;

    public FastCacheAsyncClient(String host, int port, int defaultClientId) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .directExecutor() // Optimization for i9: bypass extra thread hops
                .build();
        this.asyncStub = FastCacheGrpcServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
    }

    public FastCacheAsyncClient(String host, int port) {
        this(host, port, 0);
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
        asyncStub.setTtl(request, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<TtlResponse> getTtlAsync(String key, int clientId) {
        CompletableFuture<TtlResponse> future = new CompletableFuture<>();
        GetRequest ttlRequest = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build();
        asyncStub.getTtl(ttlRequest, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<TtlResponse> getTtlAsync(String key) {
        return getTtlAsync(key, defaultClientId);
    }

    // --- SECTION 1: Standard Key-Value (Unary) ---
    public CompletableFuture<byte[]> getAndDeleteValueAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build();
        asyncStub.getAndDeleteValue(request, new DecompressingObserver(future));
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
        asyncStub.createKeyValue(req, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<KeyHintResponse> createKeyAsync(String key, byte[] value) {
        return createKeyAsync(key, value, defaultClientId);
    }

    public CompletableFuture<byte[]> getValueAsync(String key, KeyHint hint, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.getValue(buildGetReq(key, hint, clientId), new DecompressingObserver(future));
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
        asyncStub.updateValue(req, new StreamObserver<>() {
            @Override
            public void onNext(UpdateValueResponse res) {
                future.complete(CompressionUtils.decompressIfNeeded(res.getValue()));
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        });
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
        asyncStub.existKey(buildGetReq(key, hint, clientId), new StreamObserver<>() {
            @Override
            public void onNext(BoolResponse res) {
                future.complete(res.getValue());
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
            }
        });
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

    public CompletableFuture<BoolResponse> removeAsync(String key, KeyHint hint, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        asyncStub.remove(buildGetReq(key, hint, clientId), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> removeAsync(String key, KeyHint hint) {
        return removeAsync(key, hint, defaultClientId);
    }

    public CompletableFuture<BoolResponse> removeAsync(String key, int clientId) {
        return removeAsync(key, null, clientId);
    }

    public CompletableFuture<BoolResponse> removeAsync(String key) {
        return removeAsync(key, null, defaultClientId);
    }

    // --- SECTION 2: TTL ---

    public CompletableFuture<BoolResponse> setTtlAsync(String key, KeyHint hint, long ttl, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        asyncStub.setTtl(buildTtlReq(key, hint, clientId, ttl), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> setTtlAsync(String key, long ttl) {
        return setTtlAsync(key, null, ttl, defaultClientId);
    }

    // --- SECTION 3: Collections (Unary) ---

    public CompletableFuture<KeyHintResponse> createQueueAsync(String key, byte[] initialValue, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateQueueRequest.Builder builder = CreateQueueRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        if (initialValue != null) {
            builder.setValue(CompressionUtils.compressIfNeeded(initialValue));
        }
        asyncStub.createQueue(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<KeyHintResponse> createQueueAsync(String key, byte[] initialValue) {
        return createQueueAsync(key, initialValue, defaultClientId);
    }
    public CompletableFuture<KeyHintResponse> createQueueAsync(String key) {
        return createQueueAsync(key, null, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createListAsync(String key, byte[] initialValue, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        if (initialValue != null) {
            builder.setValue(CompressionUtils.compressIfNeeded(initialValue));
        }
        asyncStub.createList(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<KeyHintResponse> createListAsync(String key, byte[] initialValue) {
        return createListAsync(key, initialValue, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createVectorAsync(String key, byte[] initialValue, int clientId) {
        CompletableFuture<KeyHintResponse> future = new CompletableFuture<>();
        CreateListRequest.Builder builder = CreateListRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setAsArray(true);
        if (initialValue != null) {
            builder.setValue(CompressionUtils.compressIfNeeded(initialValue));
        }
        asyncStub.createList(builder.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveFrontAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.getAndRemoveFront(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                                    new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveFrontAsync(String key) {
        return getAndRemoveFrontAsync(key, defaultClientId);
    }

    public CompletableFuture<KeyHintResponse> createVectorAsync(String key, byte[] initialValue) {
        return createVectorAsync(key, initialValue, defaultClientId);
    }



    public CompletableFuture<BoolResponse> addElementToTailAsync(String key,
                                                                 KeyHint hint,
                                                                 List<byte[]> data,
                                                                 int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        AddToRequest.Builder b = AddToRequest.newBuilder().setKey(buildKey(key, hint, clientId));
        data.stream().map(CompressionUtils::compressIfNeeded).forEach(b::addValue);
        asyncStub.addElementToTail(b.build(), new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> addElementToTailAsync(String key, List<byte[]> data) {
        return addElementToTailAsync(key, null, data, defaultClientId);
    }

    public CompletableFuture<byte[]> getElementAtPositionAsync(String key, KeyHint hint, int pos, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest req = KeyPositionRequest.newBuilder()
                .setKey(buildKey(key, hint, clientId))
                .setPos(pos)
                .build();
        asyncStub.getElementAtPosition(req, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getElementAtPositionAsync(String key, int pos) {
        return getElementAtPositionAsync(key, null, pos, defaultClientId);
    }

    // --- SECTION 4: Collections (Streaming) ---

    public void streamList(String key,
                           KeyHint hint,
                           int clientId,
                           Consumer<List<byte[]>> onBatch,
                           Consumer<Throwable> onError,
                           Runnable onComplete) {
        asyncStub.getList(buildGetReq(key, hint, clientId), new StreamBatchObserver(onBatch, onError, onComplete));
    }

    public void streamList(String key,
                           Consumer<List<byte[]>> onBatch,
                           Consumer<Throwable> onError,
                           Runnable onComplete) {
        streamList(key, null, defaultClientId, onBatch, onError, onComplete);
    }

//    public void streamAndRemoveElementInRange(String key,
//                                              KeyHint hint,
//                                              boolean isArray,
//                                              int start,
//                                              int end,
//                                              int clientId,
//                                              Consumer<byte[]> onElement,
//                                              Consumer<Throwable> onError,
//                                              Runnable onComplete) {
//        KeyRangeRequest req = KeyRangeRequest.newBuilder()
//                .setKey(buildKey(key, hint, clientId))
//                .setStart(start)
//                .setEnd(end)
//                .build();
//        StreamObserver<ValueResponse> obs = new StreamObserver<>() {
//            @Override
//            public void onNext(ValueResponse v) {
//                onElement.accept(CompressionUtils.decompressIfNeeded(v.getValue()));
//            }
//
//            @Override
//            public void onError(Throwable t) {
//                onError.accept(t);
//            }
//
//            @Override
//            public void onCompleted() {
//                onComplete.run();
//            }
//        };
//        if (isArray) {
//            asyncStub.getAndRemoveElementInRangeVector(req, obs);
//        } else {
//            asyncStub.getAndRemoveElementInRangeList(req, obs);
//        }
//    }

    // --- SECTION 5: Locking ---

    public CompletableFuture<LockResponse> lockObjectAsync(String key, LockType type, int clientId, int duration) {
        CompletableFuture<LockResponse> future = new CompletableFuture<>();
        LockRequest req = LockRequest.newBuilder()
                .setKey(buildKey(key, null, clientId))
                .setLockType(type)
                .setClientId(clientId)
                .setLockDuration(duration)
                .build();
        asyncStub.lockObject(req, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<UnlockResponse> unLockObjectAsync(String key, int clientId) {
        CompletableFuture<UnlockResponse> future = new CompletableFuture<>();
        UnLockRequest req = UnLockRequest.newBuilder()
                .setKey(buildKey(key, null, clientId))
                .setClientId(clientId)
                .build();
        asyncStub.unlockObject(req, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<UnlockResponse> unLockObjectAsync(String key) {
        return unLockObjectAsync(key, defaultClientId);
    }

    public void streamElementInRange(String key,
                                     boolean isArray,
                                     int start,
                                     int end,
                                     int clientId,
                                     Consumer<List<byte[]>> onBatch,
                                     Consumer<Throwable> onError,
                                     Runnable onComplete) {
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(start)
                .setEnd(end)
                .build();
        if (isArray) {
            asyncStub.getElementInRangeVector(request, new StreamBatchObserver(onBatch, onError, onComplete));
        } else {
            asyncStub.getElementInRange(request, new StreamBatchObserver(onBatch, onError, onComplete));
        }
    }

    public void streamElementInRange(String key,
                                     boolean isArray,
                                     int start,
                                     int end,
                                     Consumer<List<byte[]>> onBatch,
                                     Consumer<Throwable> onError,
                                     Runnable onComplete) {
        streamElementInRange(key, isArray, start, end, defaultClientId, onBatch, onError, onComplete);
    }

    public void streamVector(String key,
                             int clientId,
                             Consumer<List<byte[]>> onBatch,
                             Consumer<Throwable> onError,
                             Runnable onComplete) {
        GetRequest request = GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build();
        asyncStub.getVector(request, new StreamBatchObserver(onBatch, onError, onComplete));
    }

    public void streamVector(String key,
                             Consumer<List<byte[]>> onBatch,
                             Consumer<Throwable> onError,
                             Runnable onComplete) {
        streamVector(key, defaultClientId, onBatch, onError, onComplete);
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPositionAsync(String key, int pos, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(pos)
                .build();
        asyncStub.getAndRemoveElementAtPosition(request, new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getAndRemoveElementAtPositionAsync(String key, int pos) {
        return getAndRemoveElementAtPositionAsync(key, pos, defaultClientId);
    }

    // --- SECTION 6: Collection Modification ---

    public CompletableFuture<BoolResponse> addElementToTailAsync(String key, List<byte[]> data, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        AddToRequest request = builder.build();
        asyncStub.addElementToTail(request, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> addElementToHeadAsync(String key, List<byte[]> data, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        AddToRequest.Builder builder = AddToRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId));
        data.stream().map(KeyUtils::createValue).forEach(builder::addValue);
        AddToRequest request = builder.build();
        asyncStub.addElementToHead(request, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> addElementToHeadAsync(String key, List<byte[]> data) {
        return addElementToHeadAsync(key, data, defaultClientId);
    }

    public CompletableFuture<BoolResponse> addElementToPositionAsync(String key, byte[] data, int pos, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        AddToRequest request = AddToRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(pos)
                .addValue(CompressionUtils.compressIfNeeded(data))
                .build();
        asyncStub.addElementToPosition(request, new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> addElementToPositionAsync(String key, byte[] data, int pos) {
        return addElementToPositionAsync(key, data, pos, defaultClientId);
    }

    public CompletableFuture<BoolResponse> removeTailAsync(String key, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        asyncStub.removeTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                             new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> removeTailAsync(String key) {
        return removeTailAsync(key, defaultClientId);
    }

    public CompletableFuture<BoolResponse> removeHeadAsync(String key, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        asyncStub.removeHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                             new CompletableFutureObserver<>(future));
        return future;
    }

    public CompletableFuture<BoolResponse> removeHeadAsync(String key) {
        return removeHeadAsync(key, defaultClientId);
    }

//    public void streamAndRemoveElementInRange(String key,
//                                              boolean isArray,
//                                              int start,
//                                              int end,
//                                              int clientId,
//                                              Consumer<byte[]> onElement,
//                                              Consumer<Throwable> onError,
//                                              Runnable onComplete) {
//        KeyRangeRequest request = KeyRangeRequest.newBuilder()
//                .setKey(KeyUtils.createKey(key, clientId))
//                .setStart(start)
//                .setEnd(end)
//                .build();
//        StreamObserver<ValueResponse> observer = new StreamObserver<>() {
//            @Override
//            public void onNext(ValueResponse value) {
//                onElement.accept(CompressionUtils.decompressIfNeeded(value.getValue()));
//            }
//
//            @Override
//            public void onError(Throwable t) {
//                onError.accept(t);
//            }
//
//            @Override
//            public void onCompleted() {
//                onComplete.run();
//            }
//        };
//
//        if (isArray) {
//            asyncStub.getAndRemoveElementInRangeVector(request, observer);
//        } else {
//            asyncStub.getAndRemoveElementInRangeList(request, observer);
//        }
//    }

    public void removeElementInRange(String key,
                                     int start,
                                     int end,
                                     int clientId,
                                     Consumer<Boolean> onNext,
                                     Consumer<Throwable> onError,
                                     Runnable onComplete) {
        KeyRangeRequest request = KeyRangeRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setStart(start)
                .setEnd(end)
                .build();
        asyncStub.removeElementInRange(request, new StreamObserver<>() {
            @Override
            public void onNext(BoolResponse value) {
                onNext.accept(value.getValue());
            }

            @Override
            public void onError(Throwable t) {
                onError.accept(t);
            }

            @Override
            public void onCompleted() {
                onComplete.run();
            }
        });
    }

    public void removeElementInRange(String key,
                                     int start,
                                     int end,
                                     Consumer<Boolean> onNext,
                                     Consumer<Throwable> onError,
                                     Runnable onComplete) {
        removeElementInRange(key, start, end, defaultClientId, onNext, onError, onComplete);
    }

//    public void streamAndRemoveElementInRange(String key,
//                                              boolean isArray,
//                                              int start,
//                                              int end,
//                                              Consumer<byte[]> onElement,
//                                              Consumer<Throwable> onError,
//                                              Runnable onComplete) {
//        streamAndRemoveElementInRange(key, isArray, start, end, defaultClientId, onElement, onError, onComplete);
//    }

    public CompletableFuture<BoolResponse> removeElementAtPositionAsync(String key, int pos) {
        return removeElementAtPositionAsync(key, pos, defaultClientId);
    }

    public CompletableFuture<BoolResponse> removeElementAtPositionAsync(String key, int pos, int clientId) {
        CompletableFuture<BoolResponse> future = new CompletableFuture<>();
        KeyPositionRequest request = KeyPositionRequest.newBuilder()
                .setKey(KeyUtils.createKey(key, clientId))
                .setPos(pos)
                .build();
        asyncStub.removeElementAtPosition(request, new CompletableFutureObserver<>(future));
        return future;
    }

    // --- LIFECYCLE ---
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public CompletableFuture<byte[]> getHeadAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.getHead(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                          new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getTailAsync(String key, int clientId) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStub.getTail(GetRequest.newBuilder().setKey(KeyUtils.createKey(key, clientId)).build(),
                          new DecompressingObserver(future));
        return future;
    }

    public CompletableFuture<byte[]> getHeadAsync(String key) {
        return getHeadAsync(key, defaultClientId);
    }

    public CompletableFuture<byte[]> getTailAsync(String key) {
        return getTailAsync(key, defaultClientId);
    }

    // --- OBSERVERS ---

    private static class CompletableFutureObserver<T> implements StreamObserver<T> {
        private final CompletableFuture<T> future;

        public CompletableFutureObserver(CompletableFuture<T> future) {
            this.future = future;
        }

        @Override
        public void onNext(T value) {
            future.complete(value);
        }

        @Override
        public void onError(Throwable t) {
            Throwable cause = (t instanceof StatusRuntimeException && t.getCause() != null)
                              ? t.getCause()
                              : t;
            future.completeExceptionally(cause);
        }

        @Override
        public void onCompleted() {
        }
    }

    private static class DecompressingObserver implements StreamObserver<ValueResponse> {
        private final CompletableFuture<byte[]> future;

        public DecompressingObserver(CompletableFuture<byte[]> future) {
            this.future = future;
        }

        @Override
        public void onNext(ValueResponse res) {
            future.complete(CompressionUtils.decompressIfNeeded(res.getValue()));
        }

        @Override
        public void onError(Throwable t) {
            Throwable cause = (t instanceof StatusRuntimeException && t.getCause() != null)
                              ? t.getCause()
                              : t;
            future.completeExceptionally(cause);
        }

        @Override
        public void onCompleted() {
        }
    }

    private static class StreamBatchObserver implements StreamObserver<BatchValueResponse> {
        private final Consumer<List<byte[]>> onBatch;
        private final Consumer<Throwable> onError;
        private final Runnable onComplete;

        public StreamBatchObserver(Consumer<List<byte[]>> onBatch, Consumer<Throwable> onError, Runnable onComplete) {
            this.onBatch = onBatch;
            this.onError = onError;
            this.onComplete = onComplete;
        }

        @Override
        public void onNext(BatchValueResponse res) {
            onBatch.accept(res.getValueList().stream().map(CompressionUtils::decompressIfNeeded).toList());
        }

        @Override
        public void onError(Throwable t) {
            Throwable cause = (t instanceof StatusRuntimeException && t.getCause() != null)
                              ? t.getCause()
                              : t;
            onError.accept(cause);
        }

        @Override
        public void onCompleted() {
            onComplete.run();
        }
    }
}