package com.fastcache.client;

import com.fastcache.grpc.AddToRequest;
import com.fastcache.grpc.BatchValueResponse;
import com.fastcache.grpc.BinaryPayload;
import com.fastcache.grpc.CreateListRequest;
import com.fastcache.grpc.CreateRequest;
import com.fastcache.grpc.FastCacheGrpcServiceGrpc;
import com.fastcache.grpc.GetRequest;
import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHintResponse;
import com.fastcache.grpc.LockRequest;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import com.fastcache.grpc.UnLockRequest;
import com.fastcache.grpc.Value;
import com.fastcache.grpc.ValueResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FastCacheClient {

    private final FastCacheGrpcServiceGrpc.FastCacheGrpcServiceBlockingStub blockingStub;
    private final ManagedChannel channel;

    public FastCacheClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext() // Use TLS for production
                .build();
        this.blockingStub = FastCacheGrpcServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    // --- Helper: Create Key object ---
    private Key buildKey(String keyStr) {
        return Key.newBuilder()
                .setPayload(BinaryPayload.newBuilder()
                                    .setPayload(ByteString.copyFromUtf8(keyStr))
                                    .setSize(keyStr.length())
                                    .build())
                .build();
    }

    // --- Helper: Create Value object ---
    private Value buildValue(byte[] data) {
        return Value.newBuilder()
                .setValue(BinaryPayload.newBuilder().setPayload(ByteString.copyFrom(data)).setSize(data.length).build())
                .build();
    }

    // --- 1. Basic KV Operations ---

    public KeyHintResponse create(String key, byte[] value) {
        CreateRequest request = CreateRequest.newBuilder().setKey(buildKey(key)).setValue(buildValue(value)).build();
        return blockingStub.createKeyValue(request);
    }

    public ValueResponse get(String key) {
        return blockingStub.getValue(GetRequest.newBuilder().setKey(buildKey(key)).build());
    }

    public boolean exists(String key) {
        return blockingStub.existKey(GetRequest.newBuilder().setKey(buildKey(key)).build()).getValue();
    }

    public boolean remove(String key) {
        return blockingStub.remove(GetRequest.newBuilder().setKey(buildKey(key)).build()).getValue();
    }

    // --- 2. Collection Operations (List/Vector) ---

    public KeyHintResponse createList(String key, List<byte[]> initialValue, boolean asArray) {
        CreateListRequest.Builder builder = CreateListRequest.newBuilder().setKey(buildKey(key)).setAsArray(asArray);

        initialValue.stream().map(CompressionUtils::compressIfNeeded).forEach(builder::addValue);

        return blockingStub.createList(builder.build());
    }

    /**
     * Handles Batch Streaming from the server
     */
    public List<byte[]> getFullList(String key) {
        List<byte[]> results = new ArrayList<>();
        GetRequest request = GetRequest.newBuilder().setKey(buildKey(key)).build();

        try {
            Iterator<BatchValueResponse> responseIterator = blockingStub.getList(request);
            while (responseIterator.hasNext()) {
                BatchValueResponse batch = responseIterator.next();
                for (Value v : batch.getValueList()) {
                    results.add(v.getValue().getPayload().toByteArray());
                }
            }
        } catch (StatusRuntimeException e) {
            System.err.println("Stream failed: " + e.getStatus());
        }
        return results;
    }

    public boolean addElementToTail(String key, byte[] value) {
        AddToRequest request = AddToRequest.newBuilder()
                .setKey(buildKey(key))
                .addValue(buildValue(value))
                .build();
        return blockingStub.addElementToTail(request).getValue();
    }

    // --- 3. Atomic Front/Tail Ops ---

    public byte[] getFront(String key, boolean remove) {
        GetRequest request = GetRequest.newBuilder().setKey(buildKey(key)).build();
        ValueResponse response = remove
                                 ? blockingStub.getAndRemoveFront(request)
                                 : blockingStub.getHead(request);
        return response.getValue().getValue().getPayload().toByteArray();
    }

    // --- 4. Locking Ops ---

    public LockStatus lock(String key, LockType type, int durationSeconds, int clientId) {
        LockRequest request = LockRequest.newBuilder()
                .setKey(buildKey(key))
                .setLockType(type)
                .setLockDuration(durationSeconds)
                .setClientId(clientId)
                .build();
        return blockingStub.lockObject(request).getResult();
    }

    public LockStatus unlock(String key, int clientId) {
        UnLockRequest request = UnLockRequest.newBuilder().setKey(buildKey(key)).setClientId(clientId).build();
        return blockingStub.unlockObject(request).getResult();
    }
}