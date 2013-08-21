#ifdef __APPLE__
	#include <libkern/OSByteOrder.h>
	#define ntohll OSSwapBigToHostInt64
	#define htonll OSSwapHostToBigInt64
#else
	#include <endian.h>
	#include <arpa/inet.h>
	#define ntohll be64toh
	#define htonll htobe64
#endif
