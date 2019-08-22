#ifndef VERSIONED_STREAM_H
#define VERSIONED_STREAM_H

#include "fdstream.h"

struct VersionedFDWriteStream: FDWriteStream {
	VersionedFDWriteStream(int fd): FDWriteStream(fd), protocol_version(0) {}
	int protocol_version;
};

#endif
