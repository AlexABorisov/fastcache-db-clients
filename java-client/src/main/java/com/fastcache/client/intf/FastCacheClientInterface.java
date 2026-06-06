package com.fastcache.client.intf;

import com.fastcache.client.KeyUtils;
import com.fastcache.grpc.KeyHint;
import com.fastcache.grpc.LockStatus;
import com.fastcache.grpc.LockType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface FastCacheClientInterface {
    int getDefaultClientId();

    Duration getDefaultTimeout();
    default Duration getDefaultTtl() {return null;}
    String getTarget();
    default byte[] serializeKey(String key){
        return key.getBytes(StandardCharsets.UTF_8);
    }

    default KeyHint getKeyHint(byte[] key) {
        return KeyHint.newBuilder().setWeekHash(KeyUtils.weekHash(key, key.length, 0)).build();
    }

    default CompletableFuture<Boolean> setTtl(String key, long ttl) {
        return setTtl(key, ttl, getDefaultClientId());
    }

    default CompletableFuture<Boolean> setTtl(String key, long ttl, int clientId) {
        return setTtl(key, null, ttl, clientId);
    }

    default CompletableFuture<Boolean> setTtl(String key, KeyHint hint, long ttl, int clientId) {
        return setTtl(serializeKey(key), hint, ttl, clientId, getDefaultTimeout());
    }
    default CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl){
        return setTtl(key,hint ,ttl,getDefaultClientId(),getDefaultTimeout());
    }
    CompletableFuture<Boolean> setTtl(byte[] key, KeyHint hint, long ttl, int clientId, Duration timeout);

    CompletableFuture<Long> getTtl(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Long> getTtl(String key) {
        return getTtl(key, getDefaultClientId());
    }

    default CompletableFuture<Long> getTtl(String key, KeyHint hint) {
        return getTtl(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(byte [] key, KeyHint hint) {
        return getTtl(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Long> getTtl(String key, int clientId) {
        return getTtl(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndDeleteValue(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndDeleteValue(String key) {
        return getAndDeleteValue(key, getDefaultClientId());
    }



    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(byte [] key, KeyHint hint) {
        return getAndDeleteValue(key, hint, getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, int clientId) {
        return getAndDeleteValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndDeleteValue(String key, KeyHint hint, int clientId) {
        return getAndDeleteValue(serializeKey(key), hint, clientId, getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createKeyValue(byte[] key, KeyHint hint, byte[] value,Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value, int clientId, Duration timeout){
        return createKeyValue(key,null,value,getDefaultTtl(),clientId,timeout);
    }


    default CompletableFuture<KeyHint> createKeyValue(String key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value) {
        return createKeyValue(key, value, getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createKeyValue(String key, byte[] value, int clientId) {
        return createKeyValue(serializeKey(key), value, clientId, getDefaultTimeout());
    }
    default CompletableFuture<KeyHint> createKeyValue(byte[] key, byte[] value, int clientId) {
        return createKeyValue(key, value, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getValue(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getValue(String key) {
        return getValue(key, getDefaultClientId());
    }
    default CompletableFuture<byte[]> getValue(byte[] key) {
        return getValue(key,getKeyHint(key), getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, KeyHint hint) {
        return getValue(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(byte[]  key, KeyHint hint) {
        return getValue(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHint hint, byte[] value) {
        return updateKeyValue(key, hint, value,getDefaultTtl(), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getValue(String key, int clientId) {
        return getValue(serializeKey(key), null, clientId, getDefaultTimeout());
    }
    default CompletableFuture<byte[]> getValue(byte[] key,KeyHint keyhint, int clientId) {
        return getValue(key, keyhint, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> updateKeyValue(byte[] key, KeyHint hint, byte[] value, Duration ttl,int clientId, Duration timeout);

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value) {
        return updateKeyValue(serializeKey(key),
                              null,
                              value,
                              getDefaultTtl(),
                              getDefaultClientId(),
                              getDefaultTimeout());
    }
    default CompletableFuture<byte[]> updateKeyValue(byte [] key,KeyHint keyHint, byte[] value,int clientID) {
        return updateKeyValue(key,
                              keyHint,
                              value,
                              getDefaultTtl(),
                              clientID,
                              getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, KeyHint hint, byte[] value) {
        return updateKeyValue(serializeKey(key),
                              hint,
                              value,
                              getDefaultTtl(),
                              getDefaultClientId(),
                              getDefaultTimeout());
    }

    default CompletableFuture<byte[]> updateKeyValue(String key, byte[] value, int clientId) {
        return updateKeyValue(serializeKey(key), null, value,getDefaultTtl(), clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> existKey(String key) {
        return existKey(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte [] key) {
        return existKey(key, getKeyHint(key), getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(String key, KeyHint hint) {
        return existKey(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> existKey(byte[] key, KeyHint hint) {
        return existKey(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Boolean> existKey(String key, int clientId) {
        return existKey(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<Boolean> remove(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> remove(String key) {
        return remove(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, KeyHint hint) {
        return remove(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(String key, int clientId) {
        return remove(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> remove(byte[] key, KeyHint hint) {
        return remove(key, hint, getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<Boolean> remove(byte[] key, KeyHint hint,int clientid) {
        return remove(key, hint, clientid, getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue,Duration ttl, int clientId, Duration timeout);

    default  CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue,Duration ttl){
        return createQueue(key,initialValue,ttl,getDefaultClientId(),getDefaultTimeout());
    }
    default CompletableFuture<KeyHint> createQueue(String key) {
        return createQueue(serializeKey(key),
                           Collections.emptyList(),
                           getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createQueue(String key, List<byte[]> initialValue) {
        return createQueue(serializeKey(key),
                           initialValue == null
                           ? Collections.emptyList()
                           : initialValue,
                           getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }
    default CompletableFuture<KeyHint> createQueue(byte[] key, List<byte[]> initialValue) {
        return createQueue(key,
                           initialValue == null
                           ? Collections.emptyList()
                           : initialValue,getDefaultTtl(),
                           getDefaultClientId(),
                           getDefaultTimeout());
    }
    
    CompletableFuture<KeyHint> createList(byte[] key, List<byte[]> initialValue,Duration ttl, int clientId, Duration timeout);
    default CompletableFuture<KeyHint> createList(byte[] key, List<byte[]> initialValue,Duration ttl)  {
        return createList(key,initialValue,ttl ,getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createList(String key) {
        return createList(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue) {
        return createList(key, initialValue, getDefaultClientId());
    }

    default CompletableFuture<KeyHint> createList(byte [] key, List<byte[]> initialValue) {
        return createList(key, initialValue,getDefaultTtl(), getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createList(String key, List<byte[]> initialValue, int clientId) {
        return createList(serializeKey(key),
                          initialValue == null
                          ? Collections.emptyList()
                          : initialValue,getDefaultTtl(),
                          clientId,
                          getDefaultTimeout());
    }

    CompletableFuture<KeyHint> createVector(byte[] key, List<byte[]> initialValue,Duration ttl, int clientId, Duration timeout);

    default CompletableFuture<KeyHint> createVector(byte[] key, List<byte[]> initialValue,Duration ttl) {
        return createVector(key, initialValue,ttl,getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createVector(String key) {
        return createVector(key, Collections.emptyList());
    }

    default CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue) {
        return createVector(key, initialValue, getDefaultClientId());
    }
    default CompletableFuture<KeyHint> createVector(byte [] key, List<byte[]> initialValue) {
        return createVector(key, initialValue, getDefaultTtl(),getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<KeyHint> createVector(String key, List<byte[]> initialValue, int clientId) {
        return createVector(serializeKey(key),
                            initialValue == null
                            ? Collections.emptyList()
                            : initialValue,getDefaultTtl(),
                            clientId,
                            getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getAndRemoveFront(String key) {
        return getAndRemoveFront(serializeKey(key),
                                      null,
                                      getDefaultClientId(),
                                      getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(String key, KeyHint hint) {
        return getAndRemoveFront(serializeKey(key),
                                      hint,
                                      getDefaultClientId(),
                                      getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(String key, int clientId) {
        return getAndRemoveFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveFront(byte[] key, KeyHint hint) {
        return getAndRemoveFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getFront(String key) {
        return getFront(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<byte[]> getFront(byte[] key) {
        return getFront(key, null, getDefaultClientId(), getDefaultTimeout());
    }
    default CompletableFuture<byte[]> getFront(String key, KeyHint hint) {
        return getFront(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(String key, int clientId) {
        return getFront(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getFront(byte[] key, KeyHint hint) {
        return getFront(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Boolean> addElementToTail(byte[] key,
                                                KeyHint hint,
                                                List<byte[]> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data) {
        return addElementToTail(key, data, getDefaultClientId());
    }


    default CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data, KeyHint hint) {
        return addElementToTail(serializeKey(key),
                                hint,
                                data,
                                getDefaultClientId(),
                                getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(String key, List<byte[]> data, int clientId) {
        return addElementToTail(serializeKey(key), null, data, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> addElementToTail(byte[] key, KeyHint hint, List<byte[]> data) {
        return addElementToTail(key, hint, data, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<byte[]> getElementAtPosition(byte[] key,
                                                   KeyHint hint,
                                                   int pos,
                                                   int clientId,
                                                   Duration timeout);

    default CompletableFuture<byte[]> getElementAtPosition(String key, int pos) {
        return getElementAtPosition(serializeKey(key),
                                    null,
                                    pos,
                                    getDefaultClientId(),
                                    getDefaultTimeout()
        );
    }

    default CompletableFuture<byte[]> getElementAtPosition(String key,KeyHint hint, int pos) {
        return getElementAtPosition(serializeKey(key),
                                    hint,
                                    pos,
                                    getDefaultClientId(),
                                    getDefaultTimeout()
        );
    }

    default CompletableFuture<byte[]> getElementAtPosition(String key, int pos, int clientId) {
        return getElementAtPosition(serializeKey(key),
                                    null,
                                    pos,
                                    clientId,
                                    getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getElementAtPosition(byte[] key, KeyHint hint, int pos) {
        return getElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamList(String key) {
        return streamList(key, getDefaultClientId());
    }

    default CompletableFuture<List<byte[]>> streamList(String key, int clientId) {
        return streamList(serializeKey(key), // Преобразуем String в byte[]
                          null,                                // KeyHint не указан
                          clientId, getDefaultTimeout()                  // Таймаут по умолчанию
        );
    }

    default CompletableFuture<List<byte[]>> streamList(String key, KeyHint hint) {
        return streamList(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamList(byte[] key, KeyHint hint) {
        return streamList(key, hint, getDefaultClientId(), getDefaultTimeout());
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

    default CompletableFuture<LockStatus> lockObject(String key, KeyHint hint, LockType type,int clientId, Duration duration) {
        return lockObject(serializeKey(key), hint, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(byte[] key, KeyHint hint, LockType type, int clientId, Duration duration) {
        return lockObject(key, hint, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, int clientId, Duration duration) {
        return lockObject(serializeKey(key), null, type, clientId, duration, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> lockObject(String key, LockType type, Duration duration) {
        return lockObject(serializeKey(key),
                          null,
                          type,
                          getDefaultClientId(),
                          duration,
                          getDefaultTimeout());
    }

    CompletableFuture<LockStatus> unlockObject(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<LockStatus> unlockObject(String key, KeyHint hint, int clientId) {
        return unlockObject(serializeKey(key), hint, clientId, getDefaultTimeout());
    }
    default CompletableFuture<LockStatus> unlockObject(byte [] key, KeyHint hint) {
        return unlockObject(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(byte [] key, KeyHint hint, int clientid) {
        return unlockObject(key, hint, clientid, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key, int clientId) {
        return unlockObject(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<LockStatus> unlockObject(String key) {
        return unlockObject(serializeKey(key), null, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<List<byte[]>> streamElementInRange(byte[] key,
                                                         KeyHint hint,
                                                         boolean isArray,
                                                         int start,
                                                         int end,
                                                         int clientId,
                                                         Duration timeout);

    default CompletableFuture<List<byte[]>> streamElementInRange(String key, boolean isArray, int start, int end) {
        return streamElementInRange(serializeKey(key),
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
        return streamElementInRange(serializeKey(key),
                                    null,
                                    isArray,
                                    start,
                                    end,
                                    clientId,
                                    getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamElementInRange(byte[] key, KeyHint hint, boolean isArray, int start, int end) {
        return streamElementInRange(key, hint, isArray, start, end, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<List<byte[]>> streamVector(String key) {
        return streamVector(key, null);
    }

    default CompletableFuture<List<byte[]>> streamVector(String key, KeyHint hint) {
        return streamVector(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamVector(String key, int clientId) {
        return streamVector(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<List<byte[]>> streamVector(byte[] key, KeyHint hint) {
        return streamVector(key, hint, getDefaultClientId(), getDefaultTimeout());
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
        return getAndRemoveElementAtPosition(serializeKey(key),
                                             null,
                                             pos,
                                             clientId,
                                             getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveElementAtPosition(byte[] key, KeyHint hint, int pos) {
        return getAndRemoveElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Boolean> addElementToHead(byte[] key,
                                                KeyHint hint,
                                                List<byte[]> data,
                                                int clientId,
                                                Duration timeout);

    default CompletableFuture<Boolean> addElementToHead(String key, List<byte[]> data) {
        return addElementToHead(serializeKey(key),
                                null,
                                data,
                                getDefaultClientId(),
                                getDefaultTimeout());
    }
    default CompletableFuture<Boolean> addElementToHead(byte[] key,KeyHint keyhint, List<byte[]> data) {
        return addElementToHead(key,
                                keyhint,
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
        return addElementToPosition(serializeKey(key), // Преобразуем ключ в byte[]
                                    null,                                // KeyHint не указан
                                    data, pos, getDefaultClientId(),               // Клиент по умолчанию
                                    getDefaultTimeout()                 // Таймаут по умолчанию
        );
    }

    default CompletableFuture<Boolean> addElementToPosition(String key, List<byte[]> data, int pos, int clientId) {
        return addElementToPosition(serializeKey(key), null, // KeyHint не указан
                                    data, pos, clientId, getDefaultTimeout() // Таймаут по умолчанию
        );
    }

    default CompletableFuture<Boolean> addElementToPosition(byte[] key, KeyHint hint, List<byte[]> data, int pos) {
        return addElementToPosition(key, hint, data, pos, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeTail(String key) {
        return removeTail(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeTail(String key, int clientId) {
        return removeTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(String key, KeyHint hint) {
        return removeTail(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeTail(byte[] key, KeyHint hint) {
        return removeTail(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<Boolean> removeHead(String key) {
        return removeHead(key, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeHead(String key, int clientId) {
        return removeHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeHead(byte[] key, KeyHint hint) {
        return removeHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<Boolean> removeElementAtPosition(byte[] key,
                                                       KeyHint hint,
                                                       int pos,
                                                       int clientId,
                                                       Duration timeout);

    default CompletableFuture<Boolean> removeElementAtPosition(String key, int pos) {
        return removeElementAtPosition(key, pos, getDefaultClientId());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key,KeyHint hint, int pos) {
        return removeElementAtPosition(serializeKey(key),
                                       hint,
                                       pos,
                                       getDefaultClientId(),
                                       getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(String key, int pos, int clientId) {
        return removeElementAtPosition(serializeKey(key),
                                       null,
                                       pos,
                                       clientId,
                                       getDefaultTimeout());
    }

    default CompletableFuture<Boolean> removeElementAtPosition(byte[] key, KeyHint hint, int pos) {
        return removeElementAtPosition(key, hint, pos, getDefaultClientId(), getDefaultTimeout());
    }

    void shutdown();

    CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getHead(String key) {
        return getHead(key, getDefaultClientId());
    }

    default CompletableFuture<byte[]> getHead(String key, int clientId) {
        return getHead(serializeKey(key), null, clientId, getDefaultTimeout());
    }
    default CompletableFuture<byte[]> getHead(String key, KeyHint hint) {
        return getHead(serializeKey(key), hint, getDefaultClientId(), getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getHead(byte[] key, KeyHint hint) {
        return getHead(key, hint, getDefaultClientId(), getDefaultTimeout());
    }

    CompletableFuture<byte[]> getTail(byte[] key, KeyHint hint, int clientId, Duration timeout);

    default CompletableFuture<byte[]> getTail(String key) {
        return getTail(key, getDefaultClientId());
    }
    default CompletableFuture<byte[]> getTail(String key,KeyHint hint) {
        return getTail(serializeKey(key),hint, getDefaultClientId(),getDefaultTimeout());
    }
    default CompletableFuture<byte[]> getTail(byte[] key,KeyHint hint) {
        return getTail(key,hint, getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    CompletableFuture<byte[]> getAndRemoveTail(byte[] key, KeyHint hint, int clientId, Duration timeout);
    default CompletableFuture<byte[]> getAndRemoveTail(String key) {
        return getTail(key, getDefaultClientId());
    }
    default CompletableFuture<byte[]> getAndRemoveTail(String key,KeyHint hint) {
        return getTail(serializeKey(key),hint, getDefaultClientId(),getDefaultTimeout());
    }
    default CompletableFuture<byte[]> getAndRemoveTail(byte[] key,KeyHint hint) {
        return getTail(key,hint, getDefaultClientId(),getDefaultTimeout());
    }

    default CompletableFuture<byte[]> getAndRemoveTail(String key, int clientId) {
        return getTail(serializeKey(key), null, clientId, getDefaultTimeout());
    }

    default KeyHint getKeyHint(byte[] key, KeyHint hint) {
        return hint == null ? getKeyHint(key) : hint;
    }


}