#include <compression.hxx>
#ifdef GZIP
#include <zlib.h>
#else
#include <libdeflate.h>
#endif

static thread_local libdeflate_compressor *shared_compressor = nullptr;
static thread_local libdeflate_decompressor *dec = nullptr;
#ifdef GZIP
char *gzip_decompress(char *source, uint32_t sourceLen, uint32_t expectedLen)
{
    z_stream stream;

    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;
    stream.avail_in = (uInt)sourceLen;
    stream.next_in = (Bytef *)source;

    // Allocate the destination buffer based on expected size
    char *dest = new char[expectedLen];
    if (!dest)
        return NULL;

    stream.avail_out = (uInt)expectedLen;
    stream.next_out = (Bytef *)dest;

    // Window bits: 15 + 16 (or 31) to decode GZip format specifically
    int windowBits = 15 + 16;
#ifdef __PREFETCH
    __builtin_prefetch(stream.next_in);
    __builtin_prefetch(stream.next_out);
#endif

    if (inflateInit2(&stream, windowBits) != Z_OK)
    {
        delete[] dest;
        return NULL;
    }

    int ret = inflate(&stream, Z_FINISH);
    inflateEnd(&stream);

    if (ret != Z_STREAM_END)
    {
        delete[] dest;
        return NULL;
    }

    return dest;
}
#endif
#ifdef GZIP
char *gzip_compress(char *source, uint32_t sourceLen, uint32_t *destLen)
{
    z_stream stream;

    // Initialize stream struct
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;
    stream.avail_in = (uInt)sourceLen;
    stream.next_in = (Bytef *)source;

    // Window bits: 15 is default.
    // Adding 16 to the window bits tells zlib to write a GZip header/trailer.
    int windowBits = 15 + 16;
    int memLevel = 8; // Default memory usage
#ifdef __PREFETCH
    __builtin_prefetch(stream.next_in);
    __builtin_prefetch(stream.next_out);
#endif

    if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                     windowBits, memLevel, Z_DEFAULT_STRATEGY) != Z_OK)
    {
        return NULL;
    }

    // GZip has a header and trailer; bound gives us the worst-case size
    size_t maxDestLen = deflateBound(&stream, sourceLen);
    char *dest = new char[maxDestLen];

    if (!dest)
    {
        delete[] dest;
        return NULL;
    }

    stream.avail_out = (uInt)maxDestLen;
    stream.next_out = (Bytef *)dest;

    // Perform compression
    int ret = deflate(&stream, Z_FINISH);

    if (ret != Z_STREAM_END)
    {
        delete[] dest;
        deflateEnd(&stream);
        return NULL;
    }

    // Final compressed size
    *destLen = stream.total_out;

    deflateEnd(&stream);
    return dest;
}
#endif
char *gzip_decompress_libdeflate(const char *source, uint32_t sourceLen, uint32_t expectedLen)
{
    char *dest = new char[expectedLen];
    if (!dest)
        return nullptr;

    if (!dec)
    {
        dec = libdeflate_alloc_decompressor();
    }

#ifdef __PREFETCH
    __builtin_prefetch(source);
    __builtin_prefetch(dest);
#endif

    enum libdeflate_result result = libdeflate_gzip_decompress(
        dec,
        source,
        sourceLen,
        dest,
        expectedLen,
        nullptr // We don't need actual_out_n since we have expectedLen
    );

    if (result != LIBDEFLATE_SUCCESS)
    {        
        delete []dest;
        return nullptr;
    }

    return dest;
}

char *gzip_compress_libdeflate(const char *source, uint32_t sourceLen, uint32_t *destLen)
{
    // 1. Initialize compressor if not already done for this thread
    if (!shared_compressor)
    {
        // Compression level 1 is recommended for high-speed caches.
        // It provides the best balance of CPU usage vs. compression ratio.
        shared_compressor = libdeflate_alloc_compressor(1);
    }

    // 2. Calculate the worst-case output size for Gzip
    size_t maxDestLen = libdeflate_gzip_compress_bound(shared_compressor, sourceLen);

    // 3. Allocate destination (Consider your Huge Page allocator here)
    char *dest = new char[maxDestLen];
    
    if (!dest)
        return nullptr;

#ifdef __PREFETCH
    // libdeflate is very fast; prefetching the source buffer helps the
    // CPU pipeline stay full during the vectorized scan.
    __builtin_prefetch(source);
    __builtin_prefetch(dest);
#endif

    // 4. Perform the actual compression
    // libdeflate_gzip_compress handles the header, deflate, and trailer in one pass.
    size_t actualSize = libdeflate_gzip_compress(
        shared_compressor,
        source,
        sourceLen,
        dest,
        maxDestLen);

    if (actualSize == 0)
    {
        // Compression failed or buffer was too small        
        delete []dest;
        return nullptr;
    }

    *destLen = static_cast<uint32_t>(actualSize);
    return dest;
}
char *decompress(char *source, uint32_t sourceLen, uint32_t expectedLen)
{
#ifdef GZIP
    return gzip_decompress(source, sourceLen, expectedLen);
#else
    return gzip_decompress_libdeflate(source, sourceLen, expectedLen);
#endif
}

char *compress(char *source, uint32_t sourceLen, uint32_t *destLen)
{
#ifdef GZIP
    return gzip_compress(source, sourceLen, destLen);
#else
    return gzip_compress_libdeflate(source, sourceLen, destLen);
#endif
}