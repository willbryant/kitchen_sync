# Kitchen Sync is all C++, but Threads requires C to be enabled to figure out what options to use
project(kitchen_sync CXX C)

cmake_minimum_required(VERSION 3.1)

# turn on debugging symbols
set(CMAKE_BUILD_TYPE Debug)

# suppress warnings about using c++11 features such as variadic templates
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++14" COMPILER_SUPPORTS_CXX14)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
CHECK_CXX_COMPILER_FLAG("-stdlib=libc++" COMPILER_SUPPORTS_STDLIB)
if(COMPILER_SUPPORTS_CXX14)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
elseif(COMPILER_SUPPORTS_CXX11)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
endif()
if(COMPILER_SUPPORTS_STDLIB)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

# optionally compile with sanitizer checks
if(SANITIZE)
	set(SANITIZE_OPTIONS "-fsanitize=address -fsanitize=undefined")

	CHECK_CXX_COMPILER_FLAG("-fsanitize=memory" COMPILER_SUPPORTS_SANITIZE_MEMORY)
	if(COMPILER_SUPPORTS_SANITIZE_MEMORY)
		set(SANITIZE_OPTIONS "${SANITIZE_OPTIONS} -fsanitize=memory")
	endif()

	CHECK_CXX_COMPILER_FLAG("-fsanitize=leak" COMPILER_SUPPORTS_SANITIZE_LEAK)
	if(COMPILER_SUPPORTS_SANITIZE_LEAK)
		set(SANITIZE_OPTIONS "${SANITIZE_OPTIONS} -fsanitize=leak")
	endif()
endif()

# where to look first for cmake modules, before ${CMAKE_ROOT}/Modules/ is checked
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/Modules)

# we will need to be linked against pthreads on most platforms, but note that we don't use REQUIRED
# here because Threads was broken (http://www.cmake.org/Bug/view.php?id=15058).
find_package(Threads)

# vendored-in version of yaml-cpp for filter files
set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "disable yaml tests")
set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "disable yaml tools")
set(YAML_CPP_BUILD_CONTRIB OFF CACHE BOOL "disable yaml contrib")
set(YAML_CPP_INSTALL OFF CACHE BOOL "disable yaml install target")
add_subdirectory(src/yaml-cpp EXCLUDE_FROM_ALL)
include_directories(src/yaml-cpp/include)
set(YamlCPP_LIBRARIES yaml-cpp)

# the main program knows nothing but how to hook up the endpoints
set(ks_SRCS src/ks.cpp src/db_url.cpp src/process.cpp src/unidirectional_pipe.cpp)
add_executable(ks ${ks_SRCS})
set_target_properties(ks PROPERTIES COMPILE_FLAGS "${SANITIZE_OPTIONS}" LINK_FLAGS "${SANITIZE_OPTIONS}")
target_link_libraries(ks ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS ks RUNTIME DESTINATION bin)

# is backtrace in libc?
if(NOT APPLE)
	find_package(Backtrace)
endif()

if(Backtrace_FOUND OR APPLE)
	ADD_DEFINITIONS("-DUSE_BACKTRACE")
endif()

# build blake3 support
include(CheckCCompilerFlag)
add_library(blake3 OBJECT src/blake3/blake3.c src/blake3/blake3_dispatch.c src/blake3/blake3_portable.c)
set_property(TARGET blake3 APPEND PROPERTY COMPILE_FLAGS "-O3")
set_property(TARGET blake3 PROPERTY C_STANDARD 99)

if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
	CHECK_C_SOURCE_COMPILES("#include <arm_neon.h>\nint main() { return 0; }" COMPILER_SUPPORTS_NEON)
else()
	CHECK_C_COMPILER_FLAG("-msse2" COMPILER_SUPPORTS_SSE2)
	CHECK_C_COMPILER_FLAG("-msse4.1" COMPILER_SUPPORTS_SSE_4_1)
	CHECK_C_COMPILER_FLAG("-mavx2" COMPILER_SUPPORTS_AVX2)
	CHECK_C_COMPILER_FLAG("-mavx512f -mavx512vl" COMPILER_SUPPORTS_AVX512)
endif()

if(COMPILER_SUPPORTS_NEON)
	target_sources(blake3 PRIVATE "src/blake3/blake3_neon.c")
else()
	set_property(TARGET blake3 APPEND PROPERTY COMPILE_DEFINITIONS "BLAKE3_NO_NEON")
endif()

if(COMPILER_SUPPORTS_SSE2)
	target_sources(blake3 PRIVATE "src/blake3/blake3_sse2.c")
	set_property(SOURCE src/blake3/blake3_sse2.c APPEND PROPERTY COMPILE_FLAGS "-msse2")
else()
	set_property(TARGET blake3 APPEND PROPERTY COMPILE_DEFINITIONS "BLAKE3_NO_SSE2")
endif()

if(COMPILER_SUPPORTS_SSE_4_1)
	target_sources(blake3 PRIVATE "src/blake3/blake3_sse41.c")
	set_property(SOURCE src/blake3/blake3_sse41.c APPEND PROPERTY COMPILE_FLAGS "-msse4.1")
else()
	set_property(TARGET blake3 APPEND PROPERTY COMPILE_DEFINITIONS "BLAKE3_NO_SSE41")
endif()

if(COMPILER_SUPPORTS_AVX2)
	target_sources(blake3 PRIVATE "src/blake3/blake3_avx2.c")
	set_property(SOURCE src/blake3/blake3_avx2.c APPEND PROPERTY COMPILE_FLAGS "-mavx2")
else()
	set_property(TARGET blake3 APPEND PROPERTY COMPILE_DEFINITIONS "BLAKE3_NO_AVX2")
endif()

if(COMPILER_SUPPORTS_AVX512)
	target_sources(blake3 PRIVATE "src/blake3/blake3_avx512.c")
	set_property(SOURCE src/blake3/blake3_avx512.c APPEND PROPERTY COMPILE_FLAGS "-mavx512f -mavx512vl")
else()
	set_property(TARGET blake3 APPEND PROPERTY COMPILE_DEFINITIONS "BLAKE3_NO_AVX512")
endif()

set(BLAKE3_OBJECTS $<TARGET_OBJECTS:blake3>)

# build xxhash support
add_library(xxhash OBJECT src/xxHash/xxhash.c)
set_property(TARGET xxhash APPEND PROPERTY COMPILE_FLAGS "-O3")
set_property(TARGET xxhash PROPERTY C_STANDARD 11)
set(XXHASH_OBJECTS $<TARGET_OBJECTS:xxhash>)

# the endpoints do the actual work
set(ks_endpoint_SRCS src/schema.cpp src/subdivision.cpp src/filters.cpp src/abortable_barrier.cpp src/md5/md5.c ${XXHASH_OBJECTS} ${BLAKE3_OBJECTS})
set(ks_endpoint_LIBS ${YamlCPP_LIBRARIES})

# we have one endpoint program for mysql
if(NOT NO_DATABASES)
	find_package(MySQL)
endif()

if(MySQL_FOUND)
	include_directories(${MySQL_INCLUDE_DIR})
	set(ks_mysql_SRCS src/ks_mysql.cpp)
	add_executable(ks_mysql ${ks_mysql_SRCS} ${ks_endpoint_SRCS})
	set_target_properties(ks_mysql PROPERTIES COMPILE_FLAGS "${SANITIZE_OPTIONS}" LINK_FLAGS "${SANITIZE_OPTIONS}")
	target_link_libraries(ks_mysql ${MySQL_LIBRARIES} ${ks_endpoint_LIBS} ${CMAKE_THREAD_LIBS_INIT})
	if(Backtrace_FOUND)
		target_link_libraries(ks_mysql ${Backtrace_LIBRARIES})
	endif()
	install(TARGETS ks_mysql RUNTIME DESTINATION bin)
endif()

# and one endpoint program for postgresql
if(NOT NO_DATABASES)
	set(PostgreSQL_ADDITIONAL_SEARCH_PATHS /usr /usr/local /opt/homebrew /opt/homebrew/opt/libpq)
	find_package(PostgreSQL)
endif()

if(PostgreSQL_FOUND)
	include_directories(${PostgreSQL_INCLUDE_DIR})
	set(ks_postgresql_SRCS src/ks_postgresql.cpp)
	add_executable(ks_postgresql ${ks_postgresql_SRCS} ${ks_endpoint_SRCS})
	set_target_properties(ks_postgresql PROPERTIES COMPILE_FLAGS "${SANITIZE_OPTIONS}" LINK_FLAGS "${SANITIZE_OPTIONS}")
	target_link_libraries(ks_postgresql ${PostgreSQL_LIBRARIES} ${ks_endpoint_LIBS} ${CMAKE_THREAD_LIBS_INIT})
	if(Backtrace_FOUND)
		target_link_libraries(ks_postgresql ${Backtrace_LIBRARIES})
	endif()
	install(TARGETS ks_postgresql RUNTIME DESTINATION bin)
endif()

# it's usually a mistake to try and compile Kitchen Sync without support for at least one database
if((NOT MySQL_FOUND) AND (NOT PostgreSQL_FOUND))
	if(NO_DATABASES)
		MESSAGE(STATUS "Compiling just the shared binary without support for any actual databases, as you requested.  Please build the other binaries separately.")
	else()
		MESSAGE(FATAL_ERROR "Couldn't find the PostgreSQL, MySQL, or MariaDB client libraries.  This would produce a build of Kitchen Sync which doesn't support any databases, which is probably not what you want.  Please see 'Compiling in support for different databases' in INSTALL.md for help.  (If you really want to compile the shared part of Kitchen Sync without support far any databases, run cmake with the -DNO_DATABASES=1 option.)")
	endif()
endif()

# the main tests require ruby (and various extra gems).  to run the suite, run
#   cmake .. && CTEST_OUTPUT_ON_FAILURE=1 make test
enable_testing()
add_subdirectory(test)
