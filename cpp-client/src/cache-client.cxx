
#include <cache-client.hxx>
#include <cache.grpc.pb.h>
#include <compression.hxx>
#include <nmmintrin.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
uint32_t week_hash(char *data, uint32_t size, uint64_t seed = 0);
uint32_t strong_hash(char *data, uint32_t size);

bool needValueCompression(ClientValue &v,ClientConfig config)
{
    return v._size > config.clientCompressionValueSizeLimit;
}

bool needKeyCompression(ClientKey &k,ClientConfig config)
{
    return k._size > config.clientCompressionKeySizeLimit;
}

bool needKeyOfload(ClientKey &k,ClientConfig config)
{
    return k._size > config.clientHashCalcKeySizeLimit;
}

bool needValueCompression(const ClientValue &v,ClientConfig config)
{
    return v._size > config.clientCompressionValueSizeLimit;
}

bool needKeyCompression(const ClientKey &k,ClientConfig config)
{
    return k._size > config.clientCompressionKeySizeLimit;
}

bool needKeyOfload(const ClientKey &k,ClientConfig config)
{
    return k._size > config.clientHashCalcKeySizeLimit;
}

ClientConfig::ClientConfig() : clientHashCalcKeySizeLimit(512), clientCompressionKeySizeLimit(512),
                               clientCompressionValueSizeLimit(1024), clientId(0) {
}

ClientKeyHint::ClientKeyHint(uint32_t w,uint32_t s):_week_hash(w),_strong_hash(s){}
ClientKeyHint::ClientKeyHint() : _week_hash(0), _strong_hash(0) {
}

CacheClient::CacheClient(const std::shared_ptr<Channel> &channel, const ClientConfig &cfg)
    : stub_(fastcache::FastCacheGrpcService::NewStub(channel)),config(cfg) {}

void keyToKeyRequest(ClientKey &key, fastcache::Key &cacheKey,ClientConfig &config)
{
    auto reqHint = cacheKey.mutable_keyhint();
    if (key.hint.has_value()){
        reqHint->set_strong_hash(key.hint.value()._strong_hash);
        reqHint->set_week_hash(key.hint.value()._week_hash);
    } else{
        if (!needKeyOfload(key,config)){
            ClientKeyHint hint(week_hash(key._data, key._size),strong_hash(key._data, key._size));
            reqHint->set_strong_hash(hint._strong_hash);
            reqHint->set_week_hash(hint._week_hash);
        }
    }
        
    auto payload = cacheKey.mutable_payload();
    if (needKeyCompression(key,config))
    {
        auto cInfo = cacheKey.mutable_compressioninfo();
        uint32_t cmpSize;
        char *out = compress(key._data, key._size, &cmpSize);
        payload->set_size(cmpSize);
        payload->set_payload(out, ((size_t)cmpSize));
        cInfo->set_enabled(true);
        cInfo->set_rawsize(key._size);
        delete [] out;
    }
    else
    {
        payload->set_size(key._size);
        payload->set_payload(key._data, ((size_t)key._size));
    }
}

void keyToKeyRequest(const ClientKey &key, fastcache::Key &cacheKey,ClientConfig &config)
{
    auto reqHint = cacheKey.mutable_keyhint();
    if (key.hint.has_value()){
        reqHint->set_strong_hash(key.hint.value()._strong_hash);
        reqHint->set_week_hash(key.hint.value()._week_hash);
    } else{
        if (!needKeyOfload(key,config)){
            ClientKeyHint hint(week_hash(key._data, key._size),strong_hash(key._data, key._size));
            reqHint->set_strong_hash(hint._strong_hash);
            reqHint->set_week_hash(hint._week_hash);
        }
    }
        
    auto payload = cacheKey.mutable_payload();
    if (needKeyCompression(key,config))
    {
        auto cInfo = cacheKey.mutable_compressioninfo();
        uint32_t cmpSize;
        char *out = compress(key._data, key._size, &cmpSize);
        payload->set_size(cmpSize);
        payload->set_payload(out, ((size_t)cmpSize));
        cInfo->set_enabled(true);
        cInfo->set_rawsize(key._size);
        delete []out;
    }
    else
    {
        payload->set_size(key._size);
        payload->set_payload(key._data, ((size_t)key._size));
    }
}

void valueToRequest(fastcache::CreateRequest &createRequest, ClientValue &val,ClientConfig config)
{
    auto valueData = createRequest.mutable_value();
    auto valuePayload = valueData->mutable_value();
    if (needValueCompression(val,config))
    {
        auto compInfo = valueData->mutable_compressioninfo();
        compInfo->set_enabled(true);
        compInfo->set_rawsize(val._size);
        uint32_t compressedSize;
        char *out = compress(val._data, val._size, &compressedSize);
        if (out == nullptr){
            std::cerr << "Client compression error sending raw message" << std::endl;
            valueData->clear_compressioninfo();
            valuePayload->set_size(val._size);
            valuePayload->set_payload(val._data, ((size_t)val._size));
        } else{
            valuePayload->set_size(compressedSize);
            valuePayload->set_payload(out, ((size_t)compressedSize));
            delete []out;
        }
        
    }
    else
    {
        valuePayload->set_size(val._size);
        valuePayload->set_payload(val._data, ((size_t)val._size));
    }
}

void valueToRequest(fastcache::CreateListRequest &createRequest, std::list<ClientValue> &val,ClientConfig config)
{
    for (auto client_value : val) {
        auto valueData = createRequest.add_value();
        auto valuePayload = valueData->mutable_value();
        if (needValueCompression(client_value,config))
        {
            auto compInfo = valueData->mutable_compressioninfo();
            compInfo->set_enabled(true);
            compInfo->set_rawsize(client_value._size);
            uint32_t compressedSize;
            char *out = compress(client_value._data, client_value._size, &compressedSize);
            if (out == nullptr){
                std::cerr << "Client compression error sending raw message" << std::endl;
                valueData->clear_compressioninfo();
                valuePayload->set_size(client_value._size);
                valuePayload->set_payload(client_value._data, ((size_t)client_value._size));
            } else{
                valuePayload->set_size(compressedSize);
                valuePayload->set_payload(out, ((size_t)compressedSize));
                delete []out;
            }

        }
        else
        {
            valuePayload->set_size(client_value._size);
            valuePayload->set_payload(client_value._data, ((size_t)client_value._size));
        }
    }


}
void valueToRequest(fastcache::CreateQueueRequest &createRequest, std::list<ClientValue> &val,ClientConfig config)
{
    for (auto client_value : val) {
        auto valueData = createRequest.add_value();
        auto valuePayload = valueData->mutable_value();
        if (needValueCompression(client_value,config))
        {
            auto compInfo = valueData->mutable_compressioninfo();
            compInfo->set_enabled(true);
            compInfo->set_rawsize(client_value._size);
            uint32_t compressedSize;
            char *out = compress(client_value._data, client_value._size, &compressedSize);
            if (out == nullptr){
                std::cerr << "Client compression error sending raw message" << std::endl;
                valueData->clear_compressioninfo();
                valuePayload->set_size(client_value._size);
                valuePayload->set_payload(client_value._data, ((size_t)client_value._size));
            } else{
                valuePayload->set_size(compressedSize);
                valuePayload->set_payload(out, ((size_t)compressedSize));
                delete []out;
            }

        }
        else
        {
            valuePayload->set_size(client_value._size);
            valuePayload->set_payload(client_value._data, ((size_t)client_value._size));
        }
    }


}


ClientValue *valueResponseToValue(const fastcache::Value &response)
{
    ClientValue *val = new ClientValue();
    val->_ownMemory = true; 
    if (response.has_compressioninfo())
    {        
        char *out = decompress(const_cast<char *>(response.value().payload().c_str()), response.value().size(), response.compressioninfo().rawsize());
        val->_size = response.compressioninfo().rawsize();
        val->_data = out; 
    }
    else
    {
        val->_size = response.value().size();
        val->_data = new char[val->_size];
        memcpy(val->_data,response.value().payload().c_str(),val->_size);
    }
    return val;
}

ClientValue *CacheClient::getByKey(ClientKey &key,bool remove)
{
    ClientContext context;
    fastcache::GetRequest cacheKey;
    keyToKeyRequest(key, *(cacheKey.mutable_key()),config);
    fastcache::ValueResponse response;
    if (!remove){
        Status s = stub_->getValue(&context, cacheKey, &response);
        return s.ok() ? valueResponseToValue(response.value()) : nullptr;
    } else {
        Status s = stub_->getAndDeleteValue(&context, cacheKey, &response);
        return s.ok() ? valueResponseToValue(response.value()) : nullptr;
    }
}
ClientValue *CacheClient::getByKey(const ClientKey &akey,bool remove)
{
    ClientContext context;
    fastcache::GetRequest cacheKey;
    keyToKeyRequest(akey, *(cacheKey.mutable_key()),config);
    fastcache::ValueResponse response;
    if (!remove){
        Status s = stub_->getValue(&context, cacheKey, &response);
        return s.ok() ? valueResponseToValue(response.value()) : nullptr;
    } else {
        Status s = stub_->getAndDeleteValue(&context, cacheKey, &response);
        return s.ok() ? valueResponseToValue(response.value()) : nullptr;
    }
}

bool CacheClient::isKeyExist(ClientKey &key)
{
    ClientContext context;
    fastcache::GetRequest cacheKey;
    keyToKeyRequest(key, *(cacheKey.mutable_key()),config);
    fastcache::BoolResponse response;
    Status s = stub_->existKey(&context, cacheKey, &response);
    return s.ok() && response.value();
}
bool CacheClient::isKeyExist(const ClientKey &key)
{
    ClientContext context;
    fastcache::GetRequest cacheKey;
    keyToKeyRequest(key, *(cacheKey.mutable_key()),config);
    fastcache::BoolResponse response;
    Status s = stub_->existKey(&context, cacheKey, &response);
    return s.ok() && response.value();
}

void CacheClient::deleteKey(ClientKey &key)
{
    ClientContext context;
    fastcache::GetRequest cacheKey;
    keyToKeyRequest(key, *(cacheKey.mutable_key()),config);
    fastcache::BoolResponse response;
    stub_->remove(&context, cacheKey, &response);
}
void CacheClient::deleteKey(const ClientKey &key)
{
    ClientContext context;
    fastcache::GetRequest cacheKey;
    keyToKeyRequest(key, *(cacheKey.mutable_key()),config);
    fastcache::BoolResponse response;
    stub_->remove(&context, cacheKey, &response);
}

bool CacheClient::createKey(ClientKey &key, ClientValue &val, ClientKeyHint *hint)
{
    ClientContext context;
    fastcache::CreateRequest createRequest;
    fastcache::Key *cacheKey = createRequest.mutable_key();
    keyToKeyRequest(key, *cacheKey,config);

    valueToRequest(createRequest, val,config);
    fastcache::KeyHintResponse response;
    auto status = stub_->createKeyValue(&context, createRequest, &response);
    if (status.ok())
    {
        if (hint != nullptr)
        {
            auto respHint = response.keyhint();
            hint->_strong_hash = respHint.strong_hash();
            hint->_week_hash = respHint.week_hash();
        }
        return true;
    }
    return false;
}
bool CacheClient::createKey(const ClientKey &key, ClientValue &val, ClientKeyHint *hint)
{
    ClientContext context;
    fastcache::CreateRequest createRequest;
    fastcache::Key *cacheKey = createRequest.mutable_key();
    keyToKeyRequest(key, *cacheKey,config);

    valueToRequest(createRequest, val,config);
    fastcache::KeyHintResponse response;
    if (stub_->createKeyValue(&context, createRequest, &response).ok())
    {
        if (hint != nullptr)
        {
            auto respHint = response.keyhint();
            hint->_strong_hash = respHint.strong_hash();
            hint->_week_hash = respHint.week_hash();
        }
        return true;
    }
    return false;
}

ClientValue::ClientValue() : _size(0), _data(nullptr), _ownMemory(false) {}

ClientValue::ClientValue(const char *data, uint32_t size, bool deepCopy)
    : _size(size), _ownMemory(deepCopy)
{
    if (deepCopy && data && size > 0)
    {
        _data = new char[_size];
#ifdef __PREFETCH
        __builtin_prefetch(&_data);
        __builtin_prefetch(&data);
#endif
        memcpy(_data, data, size);
    }
    else
    {
        _data = const_cast<char *>(data);
    }
}

ClientValue::ClientValue(char *data, uint32_t size, bool deepCopy)
    : _size(size), _ownMemory(deepCopy)
{
    if (deepCopy && data && size > 0)
    {
        _data = new char[_size];
#ifdef __PREFETCH
        __builtin_prefetch(&_data);
        __builtin_prefetch(&data);
#endif
        memcpy(_data, data, size);
    }
    else
    {
        _data = data;
    }
}

ClientValue::~ClientValue()
{
    if (_ownMemory && _data)
    {
        delete []_data;
        _data = nullptr;
    }
}

ClientValue &ClientValue::operator=(const ClientValue &other)
{
    if (this != &other)
    {
        // Clean up existing owned memory before overwriting
        if (_ownMemory && _data)
            delete []_data;

        _size = other._size;
        _data = other._data;
        _ownMemory = false;
    }
    return *this;
}

ClientValue &ClientValue::operator=(ClientValue &other)
{
    if (this != &other)
    {
        if (_ownMemory && _data)
            delete []_data;

        _size = other._size;
        _data = other._data;
        _ownMemory = false;
    }
    return *this;
}

ClientValue::ClientValue(const ClientValue &other) : _size(other._size), _data(other._data), _ownMemory(false)
{
}

ClientValue::ClientValue(ClientValue &&other) noexcept
    : _size(other._size), _data(other._data), _ownMemory(other._ownMemory)
{

    other._data = nullptr;
    other._size = 0;
    other._ownMemory = false;
}

ClientValue &ClientValue::operator=(ClientValue &&other) noexcept {
    if (this != &other)
    {
        if (_ownMemory && _data)
        {
            delete []_data;
        }

        _size = other._size;
        _data = other._data;
        _ownMemory = other._ownMemory;

        other._data = nullptr;
        other._size = 0;
        other._ownMemory = false;
    }
    return *this;
}

ClientKey::ClientKey()
    : _size(0), _data(nullptr), _ownMemory(false), hint(std::nullopt) {}

ClientKey::ClientKey(const char *data, uint32_t size, bool deepCopy)
    : _size(size), _ownMemory(deepCopy), hint(std::nullopt)
{
    if (deepCopy && data && size > 0)
    {
        _data = new char[_size];
#ifdef __PREFETCH
        __builtin_prefetch(&_data);
        __builtin_prefetch(&data);
#endif
        memcpy(_data, data, size);
    }
    else
    {
        _data = const_cast<char *>(data);
    }
}


ClientKey::ClientKey(char *data, uint32_t size, bool deepCopy)
    : _size(size), _ownMemory(deepCopy), hint(std::nullopt)
{
    if (deepCopy && data && size > 0)
    {
        _data = new char[_size];
#ifdef __PREFETCH
        __builtin_prefetch(&_data);
        __builtin_prefetch(&data);
#endif
        memcpy(_data, data, size);
    }
    else
    {
        _data = data;
    }
}

ClientKey::~ClientKey()
{
    if (_ownMemory && _data)
    {
        delete []_data;
        _data = nullptr;
    }
}

ClientKey::ClientKey(const ClientKey &other):_size(other._size),_data(other._data),_ownMemory(false),hint(other.hint)
{
}

ClientKey &ClientKey::operator=(const ClientKey &other)
{
    if (this != &other)
    {
        if (_ownMemory && _data)
        {
            delete []_data;
        }

        _size = other._size;
        _data = other._data;
        _ownMemory = false;
        hint = other.hint;
    }
    return *this;
}

ClientKey &ClientKey::operator=(ClientKey &other)
{
    if (this != &other)
    {
        if (_ownMemory && _data)
        {
            delete []_data;
        }

        _size = other._size;
        _data = other._data;
        _ownMemory = false;
        hint = other.hint;
    }
    return *this;
}
ClientKey ClientKey::clone() const
{
    ClientKey clonedCopy(this->_data, this->_size, true);

    clonedCopy.hint = this->hint;

    return clonedCopy;
}


std::vector<ClientValue*> CacheClient::getList(const ClientKey &key) {
    ClientContext context;
    fastcache::GetRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);

    auto reader = stub_->getList(&context, request);
    fastcache::BatchValueResponse batch;
    std::vector<ClientValue*> results;

    while (reader->Read(&batch)) {
        for (int i = 0; i < batch.value_size(); ++i) {
            results.push_back(valueResponseToValue(batch.value(i)));
        }
    }
    Status s = reader->Finish();
    return results;
}

std::vector<ClientValue*> CacheClient::getVector(const ClientKey &key) {
    ClientContext context;
    fastcache::GetRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);

    auto reader = stub_->getVector(&context, request);
    fastcache::BatchValueResponse batch;
    std::vector<ClientValue*> results;

    while (reader->Read(&batch)) {
        for (int i = 0; i < batch.value_size(); ++i) {
            results.push_back(valueResponseToValue(batch.value(i)));
        }
    }
    reader->Finish();
    return results;
}

ClientValue* CacheClient::getHead(const ClientKey &key, bool remove) {
    ClientContext context;
    fastcache::GetRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    fastcache::ValueResponse response;
    Status s;
    if (remove) s = stub_->getAndRemoveFront(&context, request, &response);
    else s = stub_->getHead(&context, request, &response);
    return s.ok() ? valueResponseToValue(response.value()) : nullptr;
}

ClientValue* CacheClient::getTail(const ClientKey &key, bool remove) {
    ClientContext context;
    fastcache::GetRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    fastcache::ValueResponse response;
    Status s;
    if (remove) s = stub_->getAndRemoveTail(&context, request, &response);
    else s = stub_->getTail(&context, request, &response);
    return s.ok() ? valueResponseToValue(response.value()) : nullptr;
}

bool CacheClient::createQueue(const ClientKey &key, std::list<ClientValue> &vals, bool asArray, ClientKeyHint *hint) {
    ClientContext context;
    fastcache::CreateQueueRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    valueToRequest(request, vals, config); // Re-use valueToRequest logic


    fastcache::KeyHintResponse response;
    Status s = stub_->createQueue(&context, request, &response);
    if (s.ok() && hint) {
        hint->_strong_hash = response.keyhint().strong_hash();
        hint->_week_hash = response.keyhint().week_hash();
    }
    return s.ok();
}

bool CacheClient::createList(const ClientKey &key, std::list<ClientValue> &vals, bool asArray, ClientKeyHint *hint) {
    ClientContext context;
    fastcache::CreateListRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    valueToRequest(request, vals, config); // Re-use valueToRequest logic
    request.set_asarray(asArray);

    fastcache::KeyHintResponse response;
    Status s = stub_->createList(&context, request, &response);
    if (s.ok() && hint) {
        hint->_strong_hash = response.keyhint().strong_hash();
        hint->_week_hash = response.keyhint().week_hash();
    }
    return s.ok();
}


CacheClient::LockResult CacheClient::lockObject(const ClientKey &key, fastcache::LockType type, uint32_t duration, const std::string& clientId) {
    ClientContext context;
    fastcache::LockRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    request.set_locktype(type);
    request.set_lockduration(duration);
    if (!clientId.empty()) request.set_clientid(config.clientId);

    fastcache::LockResponse response;
    Status s = stub_->lockObject(&context, request, &response);
    if (s.ok()){
        return { response.result(), response.message() };
    } else {
        return {fastcache::LockStatus::GENERIC_ERROR,s.error_message()};
    }
    
}

bool CacheClient::getTtl(const ClientKey &key,uint64_t &ttl) {
    ClientContext context;
    fastcache::GetRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    fastcache::TtlResponse response;
    auto status = stub_->getTtl(&context,request,&response);
    if (status.ok()) {
        ttl = response.ttl();
        return true;
    }
    return false;
}

bool CacheClient::setTtl(ClientKey &key, uint64_t value) {
    ClientContext context;
    fastcache::TtlRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    request.set_ttl(value);
    fastcache::BoolResponse response;
    auto status = stub_->setTtl(&context,request,&response);
    return  status.ok();
}

CacheClient::LockResult CacheClient::unlockObject(const ClientKey &key, const std::string& clientId) {
    ClientContext context;
    fastcache::UnLockRequest request;
    keyToKeyRequest(key, *(request.mutable_key()), config);
    if (!clientId.empty()) request.set_clientid(config.clientId);

    fastcache::UnlockResponse response;
    Status s = stub_->unlockObject(&context, request, &response);
    if (s.ok()){
        return { response.result(), response.message() };
    } else {
        return {fastcache::LockStatus::GENERIC_ERROR,s.error_message()};
    }
}


uint32_t strong_hash(char *data, uint32_t size)
{
    uint32_t crc = 0;
    char *sample = data;
#ifdef __PREFETCH
    __builtin_prefetch(sample);
#endif

    for (uint32_t tail = size; tail > 0;)
    {
        if (tail >= 8)
        {
            crc = _mm_crc32_u64(crc, *((long long *)sample));
            tail -= 8;
            sample += 8;
            continue;
        }
        if (tail >= 4)
        {
            crc = _mm_crc32_u32(crc, *((int *)sample));
            tail -= 4;
            sample += 4;
            continue;
        }
        if (tail >= 2)
        {
            crc = _mm_crc32_u16(crc, *((short *)sample));
            tail -= 2;
            sample += 2;
            continue;
        }
        if (tail >= 1)
        {
            crc = _mm_crc32_u8(crc, *((char *)sample));
            tail -= 1;
            sample += 1;
            continue;
        }
    }
    return crc;
}

// Essential 64-bit mix constants for t1ha
static constexpr uint64_t Prime_0 = 0xEC997CE353AD73EDull;
static constexpr uint64_t Prime_1 = 0xBE1F145728F2090Dull;

// Rotates 64-bit integer
inline uint64_t rot64(uint64_t v, int s)
{
    return (v >> s) | (v << (64 - s));
}

// 64x64 -> 128 bit multiplication helper
inline uint64_t mux64(uint64_t v, uint64_t p, uint64_t &upper)
{
#if defined(__SIZEOF_INT128__)
    unsigned __int128 r = (unsigned __int128)v * p;
    upper = (uint64_t)(r >> 64);
    return (uint64_t)r;
#else
    // Fallback for compilers without 128-bit int
    uint64_t l = (v & 0xFFFFFFFF) * (p & 0xFFFFFFFF);
    uint64_t m1 = (v >> 32) * (p & 0xFFFFFFFF);
    uint64_t m2 = (v & 0xFFFFFFFF) * (p >> 32);
    uint64_t h = (v >> 32) * (p >> 32);
    uint64_t mm = m1 + m2 + (l >> 32);
    upper = h + (mm >> 32);
    return (l & 0xFFFFFFFF) | (mm << 32);
#endif
}

uint32_t week_hash(char *data, uint32_t len, uint64_t seed)
{

    const uint8_t *ptr = (uint8_t *)data;
#ifdef __PREFETCH
    __builtin_prefetch(ptr);
#endif

    uint64_t a = seed ^ Prime_0;
    uint64_t b = len ^ Prime_1;

    // Simple 64-bit block processing
    while (len >= 8)
    {
        uint64_t val = *((uint64_t *)ptr);
        uint64_t high;
        uint64_t low = mux64(val ^ a, Prime_0, high);
        a ^= high;
        b += low;
        ptr += 8;
        len -= 8;
    }

    if (len > 4)
    {
        uint32_t tail = *((uint32_t *)ptr);
        b ^= tail;
        ptr += 4;
        len -= 4;
    }

    if (len > 2)
    {
        uint16_t tail = *((uint16_t *)ptr);
        b ^= tail;
        ptr += 2;
        len -= 2;
    }

    if (len >= 1)
    {
        uint8_t tail = *((uint8_t *)ptr);
        b ^= tail;
    }

    uint64_t final_high;
    uint64_t final_low = mux64(a ^ rot64(b, 17), Prime_1, final_high);
    return final_low ^ final_high;
}
