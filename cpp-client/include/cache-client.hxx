#ifndef CACHE_CLIENT
#define CACHE_CLIENT

#include <grpc++/grpc++.h>
#include <cache.grpc.pb.h>
#include <optional>
#include <iomanip>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

struct ClientKeyHint
{
  uint32_t _week_hash;
  uint32_t _strong_hash;
  ClientKeyHint();
  ClientKeyHint(uint32_t,uint32_t);
};

inline std::ostream &operator<<(std::ostream &os, const ClientKeyHint &k)
{
  return os << " hint=w=" << k._week_hash << " s=" << k._strong_hash;
}

struct ClientValue
{
  uint32_t _size;
  char *_data;
  bool _ownMemory;
  std::optional<uint64_t> ttl;
  ClientValue();
  ClientValue(const char *data, uint32_t size, bool deepCopy = false);
  ClientValue(char *data, uint32_t size, bool deepCopy = false);
  ~ClientValue();
  ClientValue &operator=(const ClientValue &other);
  ClientValue &operator=(ClientValue &other);
  ClientValue(const ClientValue &other);
  ClientValue(ClientValue &&other) noexcept;
  ClientValue &operator=(ClientValue &&other) noexcept;
};

inline std::ostream &operator<<(std::ostream &os, const ClientValue &cv)
{
  if (!cv._data)
    return os << "[null]";

  std::ios_base::fmtflags f(os.flags());

  os << "ClientValue(" << cv._size << " bytes): [";
  for (uint32_t i = 0; i < cv._size; ++i)
  {
    os << std::hex << std::setw(2) << std::setfill('0')
       << (static_cast<int>(cv._data[i]) & 0xFF) << " ";
  }
  os << "]";

  // Restore stream state
  os.flags(f);
  return os;
}

struct ClientKey
{
  uint32_t _size;
  char *_data;
  bool _ownMemory;
  std::optional<ClientKeyHint> hint;
  ClientKey();
  ClientKey(const char *data, uint32_t size, bool deepCopy = false);
  ClientKey(char *data, uint32_t size, bool deepCopy = false);
  ClientKey(const ClientKey &other);
  ClientKey &operator=(const ClientKey &other);
  ClientKey &operator=(ClientKey &other);
  ClientKey clone() const;
  ~ClientKey();
};

inline std::ostream &operator<<(std::ostream &os, const ClientKey &k)
{
  if (k._size != 0)
  {
    os << " k=" << std::string(k._data, k._size) << " size=" << k._size << " own-memory=" << (k._ownMemory ? "true" : "false");
    if (k.hint.has_value())
    {
      os << k.hint.value();
    }
    return os;
  }
  return os << "Empty key";
}

struct ClientConfig{
  uint32_t clientHashCalcKeySizeLimit;
  uint32_t clientCompressionKeySizeLimit;
  uint32_t clientCompressionValueSizeLimit;
  uint32_t clientId;
  ClientConfig();
};

static ClientConfig defaultConfig;

class CacheClient
{
public:
  CacheClient(const std::shared_ptr<Channel> &channel, const ClientConfig &config=defaultConfig);
  ClientValue *getByKey(ClientKey &key, bool remove=false);
  ClientValue *getByKey(const ClientKey &key, bool remove=false);

  bool isKeyExist(ClientKey &key);
  bool isKeyExist(const ClientKey &key);

  void deleteKey(ClientKey &key);
  void deleteKey(const ClientKey &key);

  bool createKey(ClientKey &key, ClientValue &val, ClientKeyHint *hint = nullptr);
  bool createKey(const ClientKey &key, ClientValue &val, ClientKeyHint *hint = nullptr);

  std::vector<ClientValue*> getList(const ClientKey &key);
  std::vector<ClientValue*> getVector(const ClientKey &key);

  ClientValue* getHead(const ClientKey &key, bool remove = false);
  ClientValue* getTail(const ClientKey &key, bool remove = false);

  bool createQueue(const ClientKey &key, std::list<ClientValue> &vals, bool asArray, ClientKeyHint *hint);

  bool createList(const ClientKey &key, std::list<ClientValue> &vals, bool asArray = false, ClientKeyHint *hint = nullptr);
  struct LockResult {
      fastcache::LockStatus lockresult;
      std::string message;
  };

  LockResult lockObject(const ClientKey &key, fastcache::LockType type, uint32_t durationSeconds = 86400, const std::string& clientId = "");
  LockResult unlockObject(const ClientKey &key, const std::string& clientId = "");

  bool getTtl(const ClientKey &key, uint64_t &ttl);

  bool setTtl(ClientKey &key, uint64_t value);

private:
  std::unique_ptr<fastcache::FastCacheGrpcService::Stub> stub_;
  ClientConfig config;
};

#endif