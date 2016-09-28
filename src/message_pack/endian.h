#ifdef __APPLE__
	#include <libkern/OSByteOrder.h>
	#ifndef ntohll
		/* added in yosemite */
		#define ntohll OSSwapBigToHostInt64
		#define htonll OSSwapHostToBigInt64
	#endif
#else
	#ifdef __FreeBSD__
		#include <sys/endian.h>
	#else
		#include <endian.h>
	#endif

	#include <arpa/inet.h>
	#define ntohll be64toh
	#define htonll htobe64
#endif
