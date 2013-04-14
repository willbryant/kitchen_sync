# - Find MessagePack
# Find the native MessagePack headers and libraries.
#
#  MessagePack_INCLUDE_DIRS - where to find msgpack.hpp, etc.
#  MessagePack_LIBRARIES    - List of libraries when using MessagePack.
#  MessagePack_FOUND        - True if MessagePack found.

find_path(MessagePack_INCLUDE_DIR NAMES msgpack.hpp)

find_library(MessagePack_LIBRARY NAMES libmsgpack msgpack)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MessagePack DEFAULT_MSG MessagePack_LIBRARY MessagePack_INCLUDE_DIR)

set(MessagePack_LIBRARIES ${MessagePack_LIBRARY})
set(MessagePack_INCLUDE_DIRS ${MessagePack_INCLUDE_DIR})

mark_as_advanced(MessagePack_LIBRARY MessagePack_INCLUDE_DIR)
