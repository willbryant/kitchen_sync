#ifndef TYPE_CODES_H
#define TYPE_CODES_H

const uint8_t MSGPACK_POSITIVE_FIXNUM_MIN = 0x00;
const uint8_t MSGPACK_POSITIVE_FIXNUM_MAX = 0x7f;
const uint8_t MSGPACK_NEGATIVE_FIXNUM_MIN = 0xe0;
const uint8_t MSGPACK_NEGATIVE_FIXNUM_MAX = 0xff;
const uint8_t MSGPACK_FIXRAW_MIN          = 0xa0;
const uint8_t MSGPACK_FIXRAW_MAX          = 0xbf;
const uint8_t MSGPACK_FIXMAP_MIN          = 0x80;
const uint8_t MSGPACK_FIXMAP_MAX          = 0x8f;
const uint8_t MSGPACK_FIXARRAY_MIN        = 0x90;
const uint8_t MSGPACK_FIXARRAY_MAX        = 0x9f;
const uint8_t MSGPACK_NIL                 = 0xc0;
const uint8_t MSGPACK_FALSE               = 0xc2;
const uint8_t MSGPACK_TRUE                = 0xc3;
const uint8_t MSGPACK_FLOAT               = 0xca;
const uint8_t MSGPACK_DOUBLE              = 0xcb;
const uint8_t MSGPACK_UINT8               = 0xcc;
const uint8_t MSGPACK_UINT16              = 0xcd;
const uint8_t MSGPACK_UINT32              = 0xce;
const uint8_t MSGPACK_UINT64              = 0xcf;
const uint8_t MSGPACK_INT8                = 0xd0;
const uint8_t MSGPACK_INT16               = 0xd1;
const uint8_t MSGPACK_INT32               = 0xd2;
const uint8_t MSGPACK_INT64               = 0xd3;
const uint8_t MSGPACK_RAW16               = 0xda;
const uint8_t MSGPACK_RAW32               = 0xdb;
const uint8_t MSGPACK_ARRAY16             = 0xdc;
const uint8_t MSGPACK_ARRAY32             = 0xdd;
const uint8_t MSGPACK_MAP16               = 0xde;
const uint8_t MSGPACK_MAP32               = 0xdf;

#endif
