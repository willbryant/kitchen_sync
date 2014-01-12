#ifndef NULLABLE_ROW_H
#define NULLABLE_ROW_H

#include "message_pack/unpack_nullable.h"

typedef Nullable<string> NullableColumnValue;
typedef vector<NullableColumnValue> NullableRow;

#endif
