package com.fastcache.client;

import com.fastcache.client.intercece.FastCacheClientInterface;
import com.fastcache.grpc.Key;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;
import com.fastcache.grpc.coordinator.CoordinatorServiceGrpc;
import com.fastcache.grpc.coordinator.NodeRole;
import com.fastcache.grpc.coordinator.PeerRouting;
import com.fastcache.grpc.coordinator.Void;
import com.fastcache.utils.Pair;
import com.fastcache.utils.RoutingObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.fastcache.grpc.coordinator.NodeRole.BACKUP;
import static com.fastcache.grpc.coordinator.NodeRole.MASTER;

public class FastCacheAsyncSmartClient implements FastCacheClientInterface {

    private static int max_shards;
    private final Mode mode;
    private final CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    static ConcurrentHashMap<Pair<NodeRole, Integer>, FastCacheClientInterface> routingTable  = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, FastCacheClientInterface> routingTableTarget = new ConcurrentHashMap<>();
    static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    static CountDownLatch readyLatch = new CountDownLatch(1);
    static AtomicBoolean readyFlag = new AtomicBoolean(false);
    static AtomicInteger randomShard = new AtomicInteger(0);


    public FastCacheAsyncSmartClient(String coordinatorHost,
                                     int coordinatorPort,
                                     int defaultClientId,
                                     Duration timeout) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
                .directExecutor()
                .usePlaintext()
                .build();
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.mode = Mode.MASTER_THAN_BACKUP;
        scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
    }


    private void init() {
        CompletableFuture<List<PeerRouting>> future = new CompletableFuture<>();
        RoutingObserver responseObserver = new RoutingObserver(future);
        asyncStub.provideGlobalRoutingInfo(Void.newBuilder().build(), responseObserver);
        try {
            List<PeerRouting> peerRouting = future.get();
            peerRouting.forEach(elem -> {
                elem.getPartitionIdsList().forEach(id -> {
                    FastCacheClientInterface fastCacheClientInterface
                            = routingTableTarget.computeIfAbsent(elem.getTarget(),
                                                                 _ -> new FastCacheAsyncSimpleClient(
                                                                         ManagedChannelBuilder.forTarget(elem.getTarget())
                                                                                 .usePlaintext()
                                                                                 .directExecutor()
                                                                                 .build(),
                                                                         defaultClientId,
                                                                         defaultTimeout));
                    routingTable.computeIfAbsent(Pair.of(elem.getRole(), id),
                                                 _ -> fastCacheClientInterface);

                });
            });
            max_shards =  responseObserver.getMaxShards();

            if (readyFlag.compareAndSet(false,true)) {
                readyLatch.countDown();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean getReadyFlag() {
        return readyFlag.get();
    }

    @Override
    public String getTarget() {
        return asyncStub.getChannel().toString();
    }

    @Override
    public int getDefaultClientId() {
        return defaultClientId;
    }

    @Override
    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    @Override
    public CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.setTtl(key, keyHint, ttl, clientId, timeout));
    }

    // --- GET TTL ---
    @Override
    public CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getTtl(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getAndDeleteValue(key, keyHint, clientId, timeout));
    }


    @Override
    public CompletableFuture<KeyHint> createKeyValue(byte[] key,KeyHint hint, byte[] value, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.createKeyValue(key,keyHint, value, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getValue(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> updateKeyValue(byte[] key,
                                                    KeyHint hint,
                                                    byte[] value,
                                                    int clientId,
                                                    Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.updateKeyValue(key, keyHint, value, clientId, timeout));
    }

    // --- KEY CHECKS & REMOVAL ---
    @Override
    public CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.existKey(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.remove(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createQueue(byte[] key,
                                                  List<byte[]> initialValue,
                                                  int clientId,
                                                  Duration timeout) {
        KeyHint keyHint = getKeyHint(key);
        return execute(keyHint, client -> client.createQueue(key, initialValue, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createList(byte[] key,
                                                 List<byte[]> initialValue,
                                                 int clientId,
                                                 Duration timeout) {
        KeyHint keyHint = getKeyHint(key);
        return execute(keyHint, client -> client.createList(key, initialValue, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createVector(byte[] key,
                                                   List<byte[]> initialValue,
                                                   int clientId,
                                                   Duration timeout) {
        KeyHint keyHint = getKeyHint(key);
        return execute(keyHint, client -> client.createVector(key, initialValue, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getAndRemoveFront(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getFront(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.addElementToTail(key, keyHint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getElementAtPosition(byte[] key,
                                                          KeyHint hint,
                                                          int pos,
                                                          int clientId,
                                                          Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getElementAtPosition(key, keyHint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.streamList(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> lockObject(byte[] key,
                                                    KeyHint hint,
                                                    LockType type,
                                                    int clientId,
                                                    Duration duration,
                                                    Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.lockObject(key, keyHint, type, clientId, duration, timeout));
    }

    @Override
    public CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.unlockObject(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                                KeyHint hint,
                                                                boolean isArray,
                                                                int start,
                                                                int end,
                                                                int clientId,
                                                                Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.streamElementInRange(key, keyHint, isArray, start, end, clientId, timeout));
    }

    @Override
    public CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.streamVector(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key,
                                                                   KeyHint hint,
                                                                   int pos,
                                                                   int clientId,
                                                                   Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getAndRemoveElementAtPosition(key, keyHint, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                       KeyHint hint,
                                                       List<byte[]> data,
                                                       int clientId,
                                                       Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.addElementToHead(key, keyHint, data, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> addElementToPosition(byte[] key,
                                                           KeyHint hint,
                                                           List<byte[]> data,
                                                           int pos,
                                                           int clientId,
                                                           Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.addElementToPosition(key, keyHint, data, pos, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.removeTail(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.removeHead(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                              KeyHint hint,
                                                              int pos,
                                                              int clientId,
                                                              Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.removeElementAtPosition(key, keyHint, pos, clientId, timeout));
    }

    @Override
    public void shutdown() {
        routingTable.values().forEach(FastCacheClientInterface::shutdown);
    }

    @Override
    public CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getHead(key, keyHint, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getTail(key, keyHint, clientId, timeout));
    }

    private FastCacheClientInterface getRoute(int shard, NodeRole role) {
        return routingTable.get(Pair.of(role,shard));
    }
    private FastCacheClientInterface getRoute(String target) {
        return routingTableTarget.get(target);
    }

    private boolean isUnavailable(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException)
                          ? ex.getCause()
                          : ex;
        return cause instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.UNAVAILABLE;
    }

    private boolean isTimeout(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException)
                          ? ex.getCause()
                          : ex;
        return cause instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    private boolean isReroute(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException)
                          ? ex.getCause()
                          : ex;
        return cause instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION;
    }
    private String getRerouteTarget(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException)
                          ? ex.getCause()
                          : ex;
        if (cause instanceof StatusRuntimeException sre && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION && sre.getTrailers() != null){
            return sre.getTrailers().get(Metadata.Key.of("x-fastcache-route",Metadata.ASCII_STRING_MARSHALLER));
        }
        return null;
    }

    private <T> CompletableFuture<T> execute(KeyHint hint,
                                             Function<FastCacheClientInterface, CompletableFuture<T>> action) {
        while (!readyFlag.get()) {
            try {
                readyLatch.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        final int shard;
        if (hint == null) {
            shard = randomShard.incrementAndGet() %max_shards;
            randomShard.compareAndSet(1023,0);
        } else {
            shard = (int)(Integer.toUnsignedLong(hint.getWeekHash())% max_shards);
        }
        final FastCacheClientInterface master = getRoute(shard, MASTER);
        final FastCacheClientInterface backup = getRoute(shard, BACKUP);
        return switch (mode) {
            case MASTER -> action.apply(master);
            case BACKUP -> action.apply(backup);
            case MASTER_THAN_BACKUP -> action.apply(master).handle((result, ex) -> {
                if (ex == null) {
                    return CompletableFuture.completedFuture(result);
                }
                if (isReroute(ex)) {
                    String case_ = "shard="+shard  +" master="+master.getTarget() + " backup="+backup.getTarget();
                    String route = getRerouteTarget(ex);
                    System.out.println("Reroute happened!" + case_ + " ->"+route);
                    return action.apply(getRoute(route));
                }
                if (isUnavailable(ex)) {
                    return action.apply(backup);
                }
                if (isTimeout(ex)) {
                    System.err.println("Timeout for shard: "+shard + " master: "+master.getTarget()+" backup: "+backup.getTarget());
                    CompletableFuture.<T>failedFuture(ex);
                }
                return CompletableFuture.<T>failedFuture(ex);
            }).thenCompose(Function.identity());
        };
    }

    public enum Mode {
        MASTER,
        MASTER_THAN_BACKUP,
        BACKUP
    }

}