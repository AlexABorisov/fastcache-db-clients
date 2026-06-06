package com.fastcache.client;

import com.fastcache.client.intf.FastCacheClientInterface;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.fastcache.grpc.coordinator.NodeRole.BACKUP;
import static com.fastcache.grpc.coordinator.NodeRole.MASTER;

public class FastCacheAsyncSmartClient implements FastCacheClientInterface {
    private final ManagedChannel channel;

    record RoutingInfo(int max_shards,
                       ConcurrentHashMap<Pair<NodeRole, Integer>, FastCacheClientInterface> routingTable,
                       ConcurrentHashMap<String, FastCacheClientInterface> routingTableTarget) {
        @Override
        public String toString() {
            return "RoutingInfo{"
                   + "max_shards="
                   + max_shards
                   + "\n routingTable="
                   + routingTable
                   + "\n routingTableTarget="
                   + routingTableTarget
                   + '}';
        }
    }

    private Mode mode;
    private final CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub;
    private final int defaultClientId;
    private final Duration defaultTimeout;
    private final Duration readyTimeout = Duration.ofSeconds(60);
    final AtomicReference<RoutingInfo> routing_info = new AtomicReference<>(new RoutingInfo(1024,
                                                                                            new ConcurrentHashMap<>(),
                                                                                            new ConcurrentHashMap<>()));
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
    CountDownLatch readyLatch = new CountDownLatch(1);
    AtomicBoolean readyFlag = new AtomicBoolean(false);
    AtomicBoolean isUpdating = new AtomicBoolean(false);
    AtomicInteger randomShard = new AtomicInteger(0);
    static Logger log = LogManager.getLogger(FastCacheAsyncSmartClient.class);

    public FastCacheAsyncSmartClient(String coordinatorHost,
                                     int coordinatorPort,
                                     int defaultClientId,
                                     Duration timeout) {
        channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
                .directExecutor()
                .usePlaintext()
                .build();
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.mode = Mode.MASTER_THAN_BACKUP;
        scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
        log.atDebug().log("Client created coordinator {}:{} timeout {}", coordinatorHost, coordinatorPort, timeout);
    }

    public FastCacheAsyncSmartClient(ManagedChannel ch, int defaultClientId, Duration timeout) {
        this.channel = ch;
        this.asyncStub = CoordinatorServiceGrpc.newStub(channel);
        this.defaultClientId = defaultClientId;
        this.defaultTimeout = timeout;
        this.mode = Mode.MASTER_THAN_BACKUP;
        scheduledExecutorService.scheduleAtFixedRate(this::init, 0, 30, TimeUnit.SECONDS);
        log.atDebug().log("Client created coordinator {} timeout {}", ch, timeout);
    }

    private void init() {
        if (!isUpdating.compareAndSet(false, true)) {
            log.debug("[INIT] Client already updating");
            return;
        }
        CompletableFuture<List<PeerRouting>> future = new CompletableFuture<>();
        RoutingObserver responseObserver = new RoutingObserver(future);

        asyncStub.provideGlobalRoutingInfo(Void.newBuilder().build(), responseObserver);
        future.orTimeout(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS).thenAccept(peerRoutingList -> {
            try {
                int maxShards = responseObserver.getMaxShards();
                RoutingInfo currentInfo = routing_info.get();

                ConcurrentHashMap<String, FastCacheClientInterface> newRoutingTableTarget = new ConcurrentHashMap<>(
                        currentInfo.routingTableTarget);
                ConcurrentHashMap<Pair<NodeRole, Integer>, FastCacheClientInterface> newRoutingTable
                        = new ConcurrentHashMap<>();
                Set<String> newTargets = peerRoutingList.stream()
                        .map(PeerRouting::getTarget)
                        .collect(Collectors.toSet());
                Set<String> oltTargets = new HashSet<>(currentInfo.routingTableTarget.keySet());
                //что добавилось
                HashSet<String> toAdd = new HashSet<>(newTargets);
                toAdd.removeAll(oltTargets);
                //что удалилось
                HashSet<String> toRemove = new HashSet<>(oltTargets);
                toRemove.removeAll(newTargets);
                //Добавим
                toAdd.forEach(item -> newRoutingTableTarget.put(item, newFastCacheClient(item)));
                toRemove.forEach(keyToRemove -> {
                    FastCacheClientInterface remove = newRoutingTableTarget.remove(keyToRemove);
                    remove.shutdown();
                });
                peerRoutingList.forEach(item -> item.getPartitionIdsList().forEach(id -> newRoutingTable.put(Pair.of(item.getRole(), id), newRoutingTableTarget.get(item.getTarget()))));
                RoutingInfo newValue = new RoutingInfo(maxShards, newRoutingTable, newRoutingTableTarget);
                routing_info.set(newValue);
                log.atDebug().log("routing_table: {}", newValue);

                // 4. Signal readiness
                if (readyFlag.compareAndSet(false, true)) {
                    log.atDebug().log("Client initialized");
                    readyLatch.countDown();
                }
            } finally {
                isUpdating.set(false);
            }
        }).exceptionally(ex -> {
            isUpdating.set(false);
            log.atWarn().log("Exception during obtaining routing information asynchronously", ex);
            return null;
        });
    }

    // Helper method to keep code clean
    private FastCacheClientInterface newFastCacheClient(String target) {
        return new FastCacheAsyncSimpleClient(ManagedChannelBuilder.forTarget(target)
                                                      .usePlaintext()
                                                      .directExecutor()
                                                      .build(), defaultClientId, defaultTimeout){
            @Override
            public Duration getDefaultTtl() {
                return FastCacheAsyncSmartClient.this.getDefaultTtl();
            }

            @Override
            public int getDefaultClientId() {
                return FastCacheAsyncSmartClient.this.getDefaultClientId();
            }

            @Override
            public Duration getDefaultTimeout() {
                return FastCacheAsyncSmartClient.this.getDefaultTimeout();
            }
        };
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
    public CompletableFuture<KeyHint> createKeyValue(byte[] key,
                                                     KeyHint hint,
                                                     byte[] value,
                                                     Duration ttl,
                                                     int clientId,
                                                     Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.createKeyValue(key, keyHint, value,ttl, clientId, timeout));
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
                                                    Duration ttl,
                                                    int clientId,
                                                    Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.updateKeyValue(key, keyHint, value,ttl, clientId, timeout));
    }

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
                                                  Duration ttl,
                                                  int clientId,
                                                  Duration timeout) {
        KeyHint keyHint = getKeyHint(key);
        return execute(keyHint, client -> client.createQueue(key, initialValue,ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createList(byte[] key,
                                                 List<byte[]> initialValue,
                                                 Duration ttl,
                                                 int clientId,
                                                 Duration timeout) {
        KeyHint keyHint = getKeyHint(key);
        return execute(keyHint, client -> client.createList(key, initialValue,ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<KeyHint> createVector(byte[] key,
                                                   List<byte[]> initialValue,
                                                   Duration ttl,
                                                   int clientId,
                                                   Duration timeout) {
        KeyHint keyHint = getKeyHint(key);
        return execute(keyHint, client -> client.createVector(key, initialValue,ttl, clientId, timeout));
    }

    @Override
    public CompletableFuture<byte[]> getAndRemoveTail(byte[] key, KeyHint hint, int clientId, Duration timeout) {
        KeyHint keyHint = getKeyHint(key, hint);
        return execute(keyHint, client -> client.getAndRemoveTail(key, keyHint, clientId, timeout));
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
        return execute(keyHint,
                       client -> client.streamElementInRange(key, keyHint, isArray, start, end, clientId, timeout));
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
        channel.shutdown();
        scheduledExecutorService.shutdown();
        routing_info.get().routingTable.values().forEach(FastCacheClientInterface::shutdown);
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

    private FastCacheClientInterface getRoute(RoutingInfo info, int shard, NodeRole role) {
        return info.routingTable.get(Pair.of(role, shard));
    }

    private FastCacheClientInterface getRoute(RoutingInfo info, String target) {
        return info.routingTableTarget.get(target);
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
        return cause instanceof StatusRuntimeException sre
               && sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    private boolean isReroute(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException)
                          ? ex.getCause()
                          : ex;
        return cause instanceof StatusRuntimeException sre
               && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION;
    }

    private String getRerouteTarget(Throwable ex) {
        Throwable cause = (ex instanceof CompletionException)
                          ? ex.getCause()
                          : ex;
        if (cause instanceof StatusRuntimeException sre
            && sre.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
            && sre.getTrailers() != null) {
            return sre.getTrailers().get(Metadata.Key.of("x-fastcache-route", Metadata.ASCII_STRING_MARSHALLER));
        }
        return null;
    }

    private <T> CompletableFuture<T> execute(KeyHint hint,
                                             Function<FastCacheClientInterface, CompletableFuture<T>> action) {
        if (!readyFlag.get()) {
            try {
                if (!readyLatch.await(readyTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Cant start client");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        final int shard;
        RoutingInfo routingInfo = routing_info.get();
        if (hint == null) {
            shard = randomShard.incrementAndGet() % routingInfo.max_shards;
        } else {
            shard = (int) (Math.abs(Integer.toUnsignedLong(hint.getWeekHash()) % routingInfo.max_shards));
        }
        Mode effectiveMode = mode;
        final FastCacheClientInterface master = getRoute(routingInfo, shard, MASTER);
        final FastCacheClientInterface backup = getRoute(routingInfo, shard, BACKUP);
        if (master == null && backup == null) {
            return CompletableFuture.failedFuture(new RuntimeException(
                    "Master and backups are both unavailable or all nodes are marked as unhealthy"));
        }
        if (master == null) {
            log.atWarn().log("Master not available routing to Backup :{}", backup.getTarget());
            effectiveMode = Mode.BACKUP;
        }

        if (backup == null) {
            log.atWarn().log("BACKUP not available routing to Master :{}", master.getTarget());
            effectiveMode = Mode.MASTER;
        }

        if (master!=null) log.atDebug().log("Selected shard: {} master: {} ", shard, master.getTarget());
        if (backup!=null)  log.atDebug().log("Selected shard: {} backup: {}", shard, backup.getTarget());

        return switch (effectiveMode) {
            case MASTER -> action.apply(master)
                    .handle(executeOnClient(shard, routingInfo, action, master))
                    .thenCompose(Function.identity());
            case BACKUP -> action.apply(backup)
                    .handle(executeOnClient(shard, routingInfo, action, backup))
                    .thenCompose(Function.identity());
            case MASTER_THAN_BACKUP -> action.apply(master).handle((result, ex) -> {
                if (ex == null) {
                    return CompletableFuture.completedFuture(result);
                }
                if (isReroute(ex)) {
                    String route = getRerouteTarget(ex);
                    log.atDebug().log("Incorrect routing table. Rerouting to {}", route, ex);
                    return action.apply(getRoute(routingInfo, route));
                }
                if (isUnavailable(ex)) {
                    log.atDebug().log("Endpoint not available {}", master.getTarget(), ex);
                    scheduledExecutorService.execute(this::init);
                    if (backup != null) {
                        return action.apply(backup);
                    }
                    return CompletableFuture.<T>failedFuture(new RuntimeException(
                            "Master unavailable and no backup configured"));
                }
                if (isTimeout(ex)) {
                    log.atDebug().log("Timeout on endpoint {}", master.getTarget(), ex);
                    CompletableFuture.<T>failedFuture(ex);
                }
                return CompletableFuture.<T>failedFuture(ex);
            }).thenCompose(Function.identity());
        };
    }

    private <T> @NonNull BiFunction<T, Throwable, CompletableFuture<T>> executeOnClient(int shard,
                                                                                        RoutingInfo routingInfo,
                                                                                        Function<FastCacheClientInterface, CompletableFuture<T>> action,
                                                                                        FastCacheClientInterface endpoint) {
        return (result, ex) -> {
            if (ex == null) {
                return CompletableFuture.completedFuture(result);
            }
            if (isReroute(ex)) {
                String route = getRerouteTarget(ex);
                log.atDebug().log("Incorrect routing table. Rerouting to {}", route, ex);
                return action.apply(getRoute(routingInfo, route));
            }
            if (isUnavailable(ex)) {
                log.atDebug().log("Endpoint not available {}", endpoint.getTarget(), ex);
                scheduledExecutorService.execute(this::init);
            }
            return CompletableFuture.failedFuture(ex);
        };
    }

    public FastCacheAsyncSmartClient setMode(Mode mode) {
        this.mode = mode;
        return this;
    }

    public enum Mode {
        MASTER,
        MASTER_THAN_BACKUP,
        BACKUP
    }

}