#ifndef DEFAULTS_H
#define DEFAULTS_H

#include "hash_algorithm.h"

const HashAlgorithm DEFAULT_HASH_ALGORITHM = HashAlgorithm::md5; // can be overridden by command-line option

const size_t DEFAULT_MINIMUM_BLOCK_SIZE =       256*1024; // arbitrary, but needs to be big enough to cope with a moderate amount of latency
const size_t DEFAULT_MAXIMUM_BLOCK_SIZE = 1024*1024*1024; // arbitrary, but needs to be small enough we don't waste unjustifiable amounts of CPU time if a block hash doesn't match

#endif
