package com.fastcache;

import com.fastcache.client.FastCacheAsyncSimpleClient;
import com.fastcache.client.FastCacheAsyncSmartClient;
import com.fastcache.client.intercece.FastCacheClientInterface;
import com.fastcache.grpc.AddToRequest;
import com.fastcache.grpc.BinaryPayload;
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
import com.fastcache.grpc.TtlRequest;
import com.fastcache.grpc.TtlResponse;
import com.fastcache.grpc.UnLockRequest;
import com.fastcache.grpc.UnlockResponse;
import com.fastcache.grpc.UpdateRequest;
import com.fastcache.grpc.UpdateValueResponse;
import com.fastcache.grpc.Value;
import com.fastcache.grpc.ValueResponse;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for in-memory gRPC tests.
 * Provides a mock FastCache server implementation for testing the client without a physical server.
 */
public abstract class TestBaseCluster {

    protected FastCacheClientInterface client;
    private Server server;

    @BeforeEach
    void setUp() throws IOException {
        String property = System.getProperty("fake", "true");
        if (Boolean.getBoolean(property)) {
            String serverName = "test-server-" + UUID.randomUUID();
            server = InProcessServerBuilder.forName(serverName).addService(new MockFastCacheService()).build().start();
            client = new FastCacheAsyncSimpleClient(InProcessChannelBuilder.forName(serverName).build());
        } else {
            client = new FastCacheAsyncSmartClient("127.0.0.1", 61000,0, Duration.ofSeconds(3600));
        }

    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            //client.shutdown();
        }
        if (server != null) {
            server.shutdown().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    /**
     * Simple in-memory mock implementation of FastCacheGrpcService.
     * Implements basic operations for testing.
     */
    public static class MockFastCacheService extends FastCacheGrpcServiceGrpc.FastCacheGrpcServiceImplBase {

        private final Map<String, Value> keyValueStore = new ConcurrentHashMap<>();
        private final Map<String, List<Value>> listStore = new ConcurrentHashMap<>();
        private final Map<String, Deque<Value>> queueStore = new ConcurrentHashMap<>();

        private String keyToString(Key key) {
            return new String(key.getPayload().getPayload().toByteArray());
        }

        private Value createValue(byte[] data) {
            return Value.newBuilder()
                    .setValue(BinaryPayload.newBuilder()
                                      .setPayload(com.google.protobuf.ByteString.copyFrom(data))
                                      .build())
                    .build();
        }

        private byte[] valueToBytes(Value value) {
            return value.getValue().getPayload().toByteArray();
        }

        @Override
        public void lockObject(LockRequest request, StreamObserver<LockResponse> responseObserver) {
            responseObserver.onNext(LockResponse.newBuilder().setResult(LockStatus.OK).build());
            responseObserver.onCompleted();
        }

        @Override
        public void unlockObject(UnLockRequest request, StreamObserver<UnlockResponse> responseObserver) {
            responseObserver.onNext(UnlockResponse.newBuilder().setResult(LockStatus.OK).build());
            responseObserver.onCompleted();
        }

        @Override
        public void setTtl(TtlRequest request, StreamObserver<BoolResponse> responseObserver) {
            // Simple TTL implementation - just acknowledge
            responseObserver.onNext(BoolResponse.newBuilder().setValue(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void getTtl(GetRequest request, StreamObserver<TtlResponse> responseObserver) {
            // Return some TTL for testing
            responseObserver.onNext(TtlResponse.newBuilder().setTtl(3600L).build());
            responseObserver.onCompleted();
        }

        @Override
        public void getValue(GetRequest request, StreamObserver<ValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            Value value = keyValueStore.get(key);
            if (value != null) {
                responseObserver.onNext(ValueResponse.newBuilder().setValue(value).build());
            } else {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void getAndDeleteValue(GetRequest request, StreamObserver<ValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            Value value = keyValueStore.remove(key);
            if (value != null) {
                responseObserver.onNext(ValueResponse.newBuilder().setValue(value).build());
            } else {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void existKey(GetRequest request, StreamObserver<BoolResponse> responseObserver) {
            String key = keyToString(request.getKey());
            boolean exists = keyValueStore.containsKey(key);
            responseObserver.onNext(BoolResponse.newBuilder().setValue(exists).build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateValue(UpdateRequest request, StreamObserver<UpdateValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            Value oldValue = keyValueStore.put(key, request.getValue());
            responseObserver.onNext(UpdateValueResponse.newBuilder()
                                            .setResult(true)
                                            .setValue(oldValue != null
                                                      ? oldValue
                                                      : Value.getDefaultInstance())
                                            .build());
            responseObserver.onCompleted();
        }

        @Override
        public void createKeyValue(CreateRequest request, StreamObserver<KeyHintResponse> responseObserver) {
            String key = keyToString(request.getKey());
            keyValueStore.put(key, request.getValue());
            KeyHint hint = KeyHint.newBuilder().setWeekHash(key.hashCode()).build();
            responseObserver.onNext(KeyHintResponse.newBuilder().setKeyHint(hint).build());
            responseObserver.onCompleted();
        }

        @Override
        public void remove(GetRequest request, StreamObserver<BoolResponse> responseObserver) {
            String key = keyToString(request.getKey());
            boolean removed = keyValueStore.remove(key) != null;
            responseObserver.onNext(BoolResponse.newBuilder().setValue(removed).build());
            responseObserver.onCompleted();
        }

        @Override
        public void createList(CreateListRequest request, StreamObserver<KeyHintResponse> responseObserver) {
            String key = keyToString(request.getKey());
            List<Value> list = new ArrayList<>();
            if (request.getValueCount() > 0) {
                list.addAll(request.getValueList());
            }
            listStore.put(key, list);
            KeyHint hint = KeyHint.newBuilder().setWeekHash(key.hashCode()).build();
            responseObserver.onNext(KeyHintResponse.newBuilder().setKeyHint(hint).build());
            responseObserver.onCompleted();
        }

        @Override
        public void createQueue(CreateQueueRequest request, StreamObserver<KeyHintResponse> responseObserver) {
            String key = keyToString(request.getKey());
            Deque<Value> queue = new ArrayDeque<>();
            if (request.getValueCount() > 0) {
                queue.addAll(request.getValueList());
            }
            queueStore.put(key, queue);
            KeyHint hint = KeyHint.newBuilder().setWeekHash(key.hashCode()).build();
            responseObserver.onNext(KeyHintResponse.newBuilder().setKeyHint(hint).build());
            responseObserver.onCompleted();
        }

        @Override
        public void getAndRemoveFront(GetRequest request, StreamObserver<ValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            Deque<Value> queue = queueStore.get(key);
            if (queue != null && !queue.isEmpty()) {
                Value value = queue.removeFirst();
                responseObserver.onNext(ValueResponse.newBuilder().setValue(value).build());
            } else {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void getTail(GetRequest request, StreamObserver<ValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            List<Value> list = listStore.get(key);
            if (list != null && !list.isEmpty()) {
                responseObserver.onNext(ValueResponse.newBuilder().setValue(list.getLast()).build());
            } else {
                Deque<Value> queue = queueStore.get(key);
                if (queue != null && !queue.isEmpty()) {
                    responseObserver.onNext(ValueResponse.newBuilder().setValue(queue.peekLast()).build());
                } else {
                    responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
                }
            }
            responseObserver.onCompleted();
        }

        @Override
        public void getHead(GetRequest request, StreamObserver<ValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            List<Value> list = listStore.get(key);
            if (list != null && !list.isEmpty()) {
                responseObserver.onNext(ValueResponse.newBuilder().setValue(list.getFirst()).build());
            } else {
                Deque<Value> queue = queueStore.get(key);
                if (queue != null && !queue.isEmpty()) {
                    responseObserver.onNext(ValueResponse.newBuilder().setValue(queue.peekFirst()).build());
                } else {
                    responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
                }
            }
            responseObserver.onCompleted();
        }

        @Override
        public void getElementAtPosition(KeyPositionRequest request, StreamObserver<ValueResponse> responseObserver) {
            String key = keyToString(request.getKey());
            List<Value> list = listStore.get(key);
            if (list != null && request.getPos() < list.size()) {
                responseObserver.onNext(ValueResponse.newBuilder().setValue(list.get(request.getPos())).build());
            } else {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
            }
            responseObserver.onCompleted();
        }

        // Implement other methods as needed for tests
        // For brevity, only implementing methods used in the tests

        @Override
        public void removeHead(GetRequest request, StreamObserver<BoolResponse> responseObserver) {
            String key = keyToString(request.getKey());
            List<Value> list = listStore.get(key);
            if (list != null && !list.isEmpty()) {
                list.removeFirst();
            } else {
                Deque<Value> queue = queueStore.get(key);
                if (queue != null && !queue.isEmpty()) {
                    queue.removeFirst();
                } else {
                    responseObserver.onNext(BoolResponse.newBuilder().setValue(false).build());
                    responseObserver.onCompleted();
                    return;
                }
            }
            responseObserver.onNext(BoolResponse.newBuilder().setValue(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void addElementToTail(AddToRequest request, StreamObserver<BoolResponse> responseObserver) {
            String key = keyToString(request.getKey());
            List<Value> list = listStore.get(key);
            if (list != null) {
                list.addAll(request.getValueList());
            } else {
                Deque<Value> queue = queueStore.get(key);
                if (queue != null) {
                    queue.addAll(request.getValueList());
                } else {
                    responseObserver.onError(io.grpc.Status.NOT_FOUND.asRuntimeException());
                    return;
                }
            }
            responseObserver.onNext(BoolResponse.newBuilder().setValue(true).build());
            responseObserver.onCompleted();
        }

        // Add more implementations as tests require them
    }
}