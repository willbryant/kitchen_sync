project(kitchen_sync CXX)

cmake_minimum_required(VERSION 2.6)

# suppress warnings about using c++11 features such as variadic templates
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
CHECK_CXX_COMPILER_FLAG("-stdlib=libc++" COMPILER_SUPPORTS_STDLIB)
if(COMPILER_SUPPORTS_CXX11)
	ADD_DEFINITIONS("-std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
	ADD_DEFINITIONS("-std=c++0x")
endif()
if(COMPILER_SUPPORTS_STDLIB)
	ADD_DEFINITIONS("-stdlib=libc++")
endif()

# where to look first for cmake modules, before ${CMAKE_ROOT}/Modules/ is checked
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/Modules)

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
    set(YAML_CPP_BUILD_TOOLS OFF)
    set(YAML_CPP_BUILD_CONTRIB OFF)
    add_subdirectory(src/yaml-cpp)
    include_directories(src/yaml-cpp/include)
    set(YamlCPP_LIBRARIES yaml-cpp)
endif()

# the main program knows nothing but how to hook up the endpoints
set(ks_SRCS src/ks.cpp src/db_url.cpp src/process.cpp src/unidirectional_pipe.cpp)
add_executable(ks ${ks_SRCS})
target_link_libraries(ks ${Boost_LIBRARIES})
install(TARGETS ks RUNTIME DESTINATION bin)

# the endpoints hash data using OpenSSL; on OS X we use Apple's Common Crypto fork instead
if(NOT APPLE)
	find_package(OpenSSL REQUIRED)
	include_directories(${OPENSSL_INCLUDE_DIRS})
endif()

# the endpoints do the actual work
set(ks_endpoint_SRCS src/schema.cpp src/filters.cpp src/abortable_barrier.cpp src/sync_queue.cpp)
set(ks_endpoint_LIBS ${OPENSSL_LIBRARIES} ${YamlCPP_LIBRARIES} ${Boost_LIBRARIES})

# turn on debugging symbols
set(CMAKE_BUILD_TYPE Debug)

# we have one endpoint program for mysql
find_package(MySQL)

if (MYSQL_FOUND)
	include_directories(${MYSQL_INCLUDE_DIR})
	set(ks_mysql_SRCS src/ks_mysql.cpp)
	add_executable(ks_mysql ${ks_mysql_SRCS} ${ks_endpoint_SRCS})
	target_link_libraries(ks_mysql ${MYSQL_LIBRARIES} ${ks_endpoint_LIBS})
	install(TARGETS ks_mysql RUNTIME DESTINATION bin)
endif()

# and one endpoint program for postgresql
set(PostgreSQL_ADDITIONAL_SEARCH_PATHS /usr/include/postgresql)
set(PostgreSQL_ADDITIONAL_VERSIONS "9.4" "9.3" "9.2")
find_package(PostgreSQL)

if (PostgreSQL_FOUND)
	include_directories(${PostgreSQL_INCLUDE_DIR})
	set(ks_postgresql_SRCS src/ks_postgresql.cpp)
	add_executable(ks_postgresql ${ks_postgresql_SRCS} ${ks_endpoint_SRCS})
	target_link_libraries(ks_postgresql ${PostgreSQL_LIBRARIES} ${ks_endpoint_LIBS})
	install(TARGETS ks_postgresql RUNTIME DESTINATION bin)
endif()

# tests require ruby and various extra gems.  to run the suite, run
#   cmake .. && CTEST_OUTPUT_ON_FAILURE=1 make test
enable_testing()
add_test(protocol_version_test   env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/protocol_version_test.rb)
add_test(snapshot_from_test      env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/snapshot_from_test.rb)
add_test(schema_from_test        env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/schema_from_test.rb)
add_test(schema_to_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/schema_to_test.rb)
add_test(hash_from_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/hash_from_test.rb)
add_test(rows_from_test          env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/rows_from_test.rb)
add_test(rows_and_hash_from_test env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/rows_and_hash_from_test.rb)
add_test(filter_from_test        env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/filter_from_test.rb)
add_test(column_types_to_test    env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/column_types_to_test.rb)
add_test(column_types_from_test  env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/column_types_from_test.rb)
add_test(sync_to_test            env BUNDLE_GEMFILE=../test/Gemfile bundle exec ruby ../test/sync_to_test.rb)
