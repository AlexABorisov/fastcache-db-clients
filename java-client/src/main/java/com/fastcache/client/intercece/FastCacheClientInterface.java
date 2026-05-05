package com.fastcache.client.intercece;

import com.fastcache.client.KeyUtils;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface FastCacheClientInterface {
    int getDefaultClientId();

    Duration getDefaultTimeout();
    String getTarget();
    default CompletableFuture<Boolean> setTtl(String key, long ttl) {
        return setTtl(key, ttl, getDefaultClientId());
    }

    default CompletableFuture<Boolean> setTtl(String key, long ttl, int clientId) {
        return setTtl(key, null, ttl, clientId);
    }

    default CompletableFuture<Boolean> setTtl(String key, KeyHint hint, long ttl, int clientId) {
        return setTtl(key.getBytes(StandardCharsets.UTF_8), hint, ttl, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout);

    CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Long> getTtl(String key) {
        return getTtl(key, getDefaultClientId());
    }

    default CompletableFuture<Long> getTtl(String key, KeyHint hint) {
        return getTtl(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(String key, int clientId) {
        return getTtl(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndDeleteValue(String key) {
        return getAndDeleteValue(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, int clientId) {
        return getAndDeleteValue(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint, int clientId) {
        return getAndDeleteValue(key.getBytes(StandardCharsets.UTF_8), hint, clientId, getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createKeyValue(byte[] key,KeyHint hint, byte[] value, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value, int clientId, Duration timeout){
        return createKeyValue(key,null,value,clientId,timeout);
    }


    default CompletableFuture<KeyHint> createKeyValue(String key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createKeyValue(String key, byte[] value, int clientId) {
        return createKeyValue(key.getBytes(StandardCharsets.UTF_8), value, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getValue(String key) {
        return getValue(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getValue(String key, KeyHint hint) {
        return getValue(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, int clientId) {
        return getValue(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHint hint, byte[] value, int clientId, Duration timeout);

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value) {
        return updateKeyValue(key.getBytes(StandardCharsets.UTF_8),
                              null,
                              value,
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, KeyHint hint, byte[] value) {
        return updateKeyValue(key.getBytes(StandardCharsets.UTF_8),
                              hint,
                              value,
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value, int clientId) {
        return updateKeyValue(key.getBytes(StandardCharsets.UTF_8), null, value, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> existKey(String key) {
        return existKey(key.getBytes(StandardCharsets.UTF_8), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, KeyHint hint) {
        return existKey(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, int clientId) {
        return existKey(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> remove(String key) {
        return remove(key.getBytes(StandardCharsets.UTF_8), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, KeyHint hint) {
        return remove(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, int clientId) {
        return remove(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createQueue(String key) {
        return createQueue(key.getBytes(StandardCharsets.UTF_8),
                           Collections.emptyList(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createQueue(String key, List<byte[]> initialValue) {
        return createQueue(key.getBytes(StandardCharsets.UTF_8),
                           initialValue == null
                           ? Collections.emptyList()
                           : initialValue,
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createList(byte[] key, List<byte[]> initialValue, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createList(String key) {
        return createList(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue) {
        return createList(key, initialValue, this.getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue, int clientId) {
        return createList(key.getBytes(StandardCharsets.UTF_8),
                          initialValue == null
                          ? Collections.emptyList()
                          : initialValue,
                          clientId,
                          this.getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createVector(byte[] key, List<byte[]> initialValue, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createVector(String key) {
        return createVector(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue) {
        return createVector(key, initialValue, this.getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue, int clientId) {
        return createVector(key.getBytes(StandardCharsets.UTF_8),
                            initialValue == null
                            ? Collections.emptyList()
                            : initialValue,
                            clientId,
                            this.getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveFront(String key) {
        return this.getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8),
                                      null,
                                      this.getDefaultClientId(),
                                      this.getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(String key, KeyHint hint) {
        return this.getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8),
                                      hint,
                                      this.getDefaultClientId(),
                                      this.getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(String key, int clientId) {
        return this.getAndRemoveFront(key.getBytes(StandardCharsets.UTF_8), null, clientId, this.getDefaultTimeout());
    }

    CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getFront(String key) {
        return getFront(key.getBytes(StandardCharsets.UTF_8), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(String key, KeyHint hint) {
        return getFront(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(String key, int clientId) {
        return getFront(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                KeyHint hint,
                                                List<byte[]> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data) {
        return addElementToTail(key, data, this.getDefaultClientId());
    }

    default CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data, KeyHint hint) {
        return addElementToTail(key.getBytes(StandardCharsets.UTF_8),
                                hint,
                                data,
                                this.getDefaultClientId(),
                                this.getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data, int clientId) {
        return addElementToTail(key.getBytes(StandardCharsets.UTF_8), null, data, clientId, this.getDefaultTimeout());
    }

    CompletableFuture<byte[]> getElementAtPosition(byte[] key,
                                                   KeyHint hint,
                                                   int pos,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<byte[]> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(key.getBytes(StandardCharsets.UTF_8),
                                    null,
                                    pos,
                                    getDefaultClientId(),
                                    getDefaultTimeout()
        );
    }

    default CompletableFuture<byte[]> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(key.getBytes(StandardCharsets.UTF_8),
                                    null,
                                    pos,
                                    clientId,
                                    getDefaultTimeout());
    }

    CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamList(String key) {
        return streamList(key, getDefaultClientId());
    }

    default CompletableFuture<List<byte[]>> streamList(String key, int clientId) {
        return streamList(key.getBytes(StandardCharsets.UTF_8), // Преобразуем String в byte[]
                          null,                                // KeyHint не указан
                          clientId, getDefaultTimeout()                  // Таймаут по умолчанию
        );
    }

    default CompletableFuture<List<byte[]>> streamList(String key, KeyHint hint) {
        return streamList(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<LockStatus> lockObject(byte[] key,
                                             KeyHint hint,
                                             LockType type,
                                             int clientId,
                                             Duration duration,
                                             Duration timeout);

    default CompletableFuture<LockStatus> lockObject(byte[] key, KeyHint hint, LockType type, Duration duration) {
        return lockObject(key, hint, type, getDefaultClientId(), duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, int clientId, Duration duration) {
        return lockObject(key.getBytes(StandardCharsets.UTF_8), null, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, Duration duration) {
        return lockObject(key.getBytes(StandardCharsets.UTF_8),
                          null,
                          type,
                          getDefaultClientId(),
                          duration,
                          getDefaultTimeout());
    }

    CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<LockStatus> unlockObject(String key, KeyHint hint, int clientId) {
        return unlockObject(key.getBytes(StandardCharsets.UTF_8), hint, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key, int clientId) {
        return unlockObject(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key) {
        return unlockObject(key.getBytes(StandardCharsets.UTF_8), null, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                         KeyHint hint,
                                                         boolean isArray,
                                                         int start,
                                                         int end,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<List<byte[]>> streamElementInRange(String key, boolean isArray, int start, int end) {
        return streamElementInRange(key.getBytes(StandardCharsets.UTF_8),
                                    null,
                                    isArray,
                                    start,
                                    end,
                                    getDefaultClientId(),
                                    getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamElementInRange(String key,
                                                                 boolean isArray,
                                                                 int start,
                                                                 int end,
                                                                 int clientId) {
        return streamElementInRange(key.getBytes(StandardCharsets.UTF_8),
                                    null,
                                    isArray,
                                    start,
                                    end,
                                    clientId,
                                    getDefaultTimeout());
    }

    CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamVector(String key) {
        return streamVector(key, null);
    }

    default CompletableFuture<List<byte[]>> streamVector(String key, KeyHint hint) {
        return streamVector(key.getBytes(StandardCharsets.UTF_8), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamVector(String key, int clientId) {
        return streamVector(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key,
                                                            KeyHint hint,
                                                            int pos,
                                                            int clientId,
                                                            Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveElementAtPosition(String key, int pos) {
        return getAndRemoveElementAtPosition(key, pos, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndRemoveElementAtPosition(String key, int pos, int clientId) {
        return getAndRemoveElementAtPosition(key.getBytes(StandardCharsets.UTF_8),
                                             null,
                                             pos,
                                             clientId,
                                             getDefaultTimeout());
    }

    CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                KeyHint hint,
                                                List<byte[]> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToHead(String key, List<byte[]> data) {
        return addElementToHead(key.getBytes(StandardCharsets.UTF_8),
                                null,
                                data,
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    CompletableFuture<Boolean> addElementToPosition(byte[] key,
                                                    KeyHint hint,
                                                    List<byte[]> data,
                                                    int pos,
                                                    int clientId,
                                                    Duration timeout);

    default CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos) {
        return addElementToPosition(key.getBytes(StandardCharsets.UTF_8), // Преобразуем ключ в byte[]
                                    null,                                // KeyHint не указан
                                    data, pos, getDefaultClientId(),               // Клиент по умолчанию
                                    getDefaultTimeout()                 // Таймаут по умолчанию
        );
    }

    default CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos, int clientId) {
        return addElementToPosition(key.getBytes(StandardCharsets.UTF_8), null, // KeyHint не указан
                                    data, pos, clientId, getDefaultTimeout() // Таймаут по умолчанию
        );
    }

    CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeTail(String key, int clientId) {
        return removeTail(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeHead(String key, int clientId) {
        return removeHead(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                       KeyHint hint,
                                                       int pos,
                                                       int clientId,
                                                       Duration timeout);

    default CompletableFuture<Boolean> removeElementAtPosition(String key, int pos) {
        return removeElementAtPosition(key, pos, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, int pos, int clientId) {
        return removeElementAtPosition(key.getBytes(StandardCharsets.UTF_8),
                                       null,
                                       pos,
                                       clientId,
                                       getDefaultTimeout());
    }

    void shutdown();

    CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getHead(String key) {
        return getHead(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getHead(String key, int clientId) {
        return getHead(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getTail(String key) {
        return getTail(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getTail(String key, int clientId) {
        return getTail(key.getBytes(StandardCharsets.UTF_8), null, clientId, getDefaultTimeout());
    }

    default KeyHint getKeyHint(byte[] key, KeyHint hint) {
        return hint == null ? getKeyHint(key) : hint;
    }

    default KeyHint getKeyHint(byte[] key) {
      return KeyHint.newBuilder().setWeekHash(KeyUtils.weekHash(key, key.length, 0)).build();
    }
}