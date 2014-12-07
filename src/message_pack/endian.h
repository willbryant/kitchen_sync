#ifdef __APPLE__
	#include <libkern/OSByteOrder.h>
	#ifndef ntohll
		/* added in yosemite */
		#define ntohll OSSwapBigToHostInt64
		#define htonll OSSwapHostToBigInt64
	#endif
#else
	#include <endian.h>
	#include <arpa/inet.h>
	#define ntohll be64toh
	#define htonll htobe64
#endif
