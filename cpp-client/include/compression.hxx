#ifndef COMPRESSION_HXX
#define COMPRESSION_HXX
#include <stdint.h>
char *decompress(char *source, uint32_t sourceLen, uint32_t expectedLen);
char *compress(char *source, uint32_t sourceLen, uint32_t *destLen);
#endif