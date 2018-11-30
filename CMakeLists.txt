# Kitchen Sync is all C++, but Threads requires C to be enabled to figure out what options to use
project(kitchen_sync CXX C)

cmake_minimum_required(VERSION 2.6)

# suppress warnings about using c++11 features such as variadic templates
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++14" COMPILER_SUPPORTS_CXX14)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
CHECK_CXX_COMPILER_FLAG("-stdlib=libc++" COMPILER_SUPPORTS_STDLIB)
if(COMPILER_SUPPORTS_CXX14)
	ADD_DEFINITIONS("-std=c++14")
elseif(COMPILER_SUPPORTS_CXX11)
	ADD_DEFINITIONS("-std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
	ADD_DEFINITIONS("-std=c++0x")
endif()
if(COMPILER_SUPPORTS_STDLIB)
	ADD_DEFINITIONS("-stdlib=libc++")
endif()

# where to look first for cmake modules, before ${CMAKE_ROOT}/Modules/ is checked
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/Modules)

# we will need to be linked against pthreads on most platforms, but note that we don't use REQUIRED
# here because Threads was broken (http://www.cmake.org/Bug/view.php?id=15058) so it may have any effect.
find_package(Threads)

# boost for a couple of algorithm and utility includes
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# yaml-cpp for filter files
include(FindPkgConfig)
pkg_check_modules(YamlCPP yaml-cpp)
if(YamlCPP_FOUND)
    include_directories(${YamlCPP_INCLUDE_DIRS})
    link_directories(${YamlCPP_LIBDIR})
else()
    set(YAML_CPP_BUILD_TESTS OFF CACHE BOOL "disable yaml tests")
    set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "disable yaml tools")
    set(YAML_CPP_BUILD_CONTRIB OFF CACHE BOOL "disable yaml contrib")
    add_subdirectory(src/yaml-cpp)
    include_directories(src/yaml-cpp/include)
    set(YamlCPP_LIBRARIES yaml-cpp)
endif()

# the main program knows nothing but how to hook up the endpoints
set(ks_SRCS src/ks.cpp src/db_url.cpp src/process.cpp src/unidirectional_pipe.cpp)
add_executable(ks ${ks_SRCS})
target_link_libraries(ks ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS ks RUNTIME DESTINATION bin)

# the endpoints hash data using OpenSSL; on OS X we use Apple's Common Crypto fork instead
if(NOT APPLE)
	find_package(OpenSSL REQUIRED)
	include_directories(${OPENSSL_INCLUDE_DIRS})
endif()

# is backtrace in libc?
if(NOT APPLE)
	find_package(Backtrace)
endif()

if(Backtrace_FOUND OR APPLE)
	ADD_DEFINITIONS("-DUSE_BACKTRACE")
endif()

# the endpoints do the actual work
set(ks_endpoint_SRCS src/schema.cpp src/filters.cpp src/abortable_barrier.cpp src/xxHash/xxhash.cpp)
set(ks_endpoint_LIBS ${OPENSSL_LIBRARIES} ${YamlCPP_LIBRARIES} ${Boost_LIBRARIES})

# turn on debugging symbols
set(CMAKE_BUILD_TYPE Debug)

# we have one endpoint program for mysql
if(NOT NO_DATABASES)
	find_package(MySQL)
endif()

if(MYSQL_FOUND)
	include_directories(${MYSQL_INCLUDE_DIR})
	set(ks_mysql_SRCS src/ks_mysql.cpp)
	add_executable(ks_mysql ${ks_mysql_SRCS} ${ks_endpoint_SRCS})
	target_link_libraries(ks_mysql ${MYSQL_LIBRARIES} ${ks_endpoint_LIBS} ${CMAKE_THREAD_LIBS_INIT})
	if(Backtrace_FOUND)
		target_link_libraries(ks_mysql ${Backtrace_LIBRARIES})
	endif()
	install(TARGETS ks_mysql RUNTIME DESTINATION bin)
endif()

# and one endpoint program for postgresql
if(NOT NO_DATABASES)
	set(PostgreSQL_ADDITIONAL_SEARCH_PATHS /usr/include/postgresql)
	set(PostgreSQL_ADDITIONAL_VERSIONS "11" "10" "9.6" "9.5" "9.4" "9.3" "9.2")
	find_package(PostgreSQL)
endif()

if(PostgreSQL_FOUND)
	include_directories(${PostgreSQL_INCLUDE_DIR})
	set(ks_postgresql_SRCS src/ks_postgresql.cpp)
	add_executable(ks_postgresql ${ks_postgresql_SRCS} ${ks_endpoint_SRCS})
	target_link_libraries(ks_postgresql ${PostgreSQL_LIBRARIES} ${ks_endpoint_LIBS} ${CMAKE_THREAD_LIBS_INIT})
	if(Backtrace_FOUND)
		target_link_libraries(ks_postgresql ${Backtrace_LIBRARIES})
	endif()
	install(TARGETS ks_postgresql RUNTIME DESTINATION bin)
endif()

# it's usually a mistake to try and compile Kitchen Sync without support for at least one database
if((NOT MYSQL_FOUND) AND (NOT PostgreSQL_FOUND))
	if(NO_DATABASES)
		MESSAGE(STATUS "Compiling just the shared binary without support for any actual databases, as you requested.  Please build the other binaries separately.")
	else()
		MESSAGE(FATAL_ERROR "Couldn't find the PostgreSQL, MySQL, or MariaDB client libraries.  This would produce a build of Kitchen Sync which didn't support any databases, which is probably not what you want.  Please see INSTALL.md for help.  (If you really want to compile the shared part of Kitchen Sync without support far any databases, use cmake with the -DNO_DATABASES=1 option.)")
	endif()
endif()

# tests require ruby and various extra gems.  to run the suite, run
#   cmake .. && CTEST_OUTPUT_ON_FAILURE=1 make test
enable_testing()
add_test(protocol_version_test   env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/protocol_version_test.rb)
add_test(snapshot_from_test      env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/snapshot_from_test.rb)
add_test(schema_from_test        env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/schema_from_test.rb)
add_test(schema_to_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/schema_to_test.rb)
add_test(range_from_test         env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/range_from_test.rb)
add_test(hash_from_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/hash_from_test.rb)
add_test(rows_from_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/rows_from_test.rb)
add_test(filter_from_file_test   env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/filter_from_file_test.rb)
add_test(filter_from_test        env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/filter_from_test.rb)
add_test(filter_to_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/filter_to_test.rb)
add_test(column_types_to_test    env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/column_types_to_test.rb)
add_test(column_types_from_test  env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/column_types_from_test.rb)
add_test(sync_to_test            env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/sync_to_test.rb)
