project(kitchen_sync CXX)

cmake_minimum_required(VERSION 2.6)

# where to look first for cmake modules, before ${CMAKE_ROOT}/Modules/ is checked
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/Modules)

# boost for program_options and threads
set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS program_options regex REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# the main program knows nothing but how to hook up the endpoints
set(ks_SRCS src/ks.cpp src/db_url.cpp src/process.cpp src/unidirectional_pipe.cpp)
add_executable(ks ${ks_SRCS})
target_link_libraries(ks ${Boost_LIBRARIES})

# the endpoints encode data using MessagePack
find_package(MessagePack REQUIRED)
include_directories(${MESSAGEPACK_INCLUDE_DIRS})

# the endpoints do the actual work
set(ks_endpoint_SRCS src/sql_functions.cpp src/schema_functions.cpp)
set(ks_endpoint_LIBS ${MessagePack_LIBRARIES})

# we have one endpoint program for mysql
find_package(MySQL REQUIRED)
include_directories(${MYSQL_INCLUDE_DIR})
set(ks_mysql_SRCS src/ks_mysql.cpp)
add_executable(ks_mysql ${ks_mysql_SRCS} ${ks_endpoint_SRCS})
target_link_libraries(ks_mysql ${MYSQL_LIBRARIES} ${ks_endpoint_LIBS})

# and one endpoint program for postgresql
set(PostgreSQL_ADDITIONAL_SEARCH_PATHS /usr/include/postgresql)
set(PostgreSQL_ADDITIONAL_VERSIONS "9.3" "9.2")
find_package(PostgreSQL REQUIRED)
include_directories(${PostgreSQL_INCLUDE_DIR})
set(ks_postgresql_SRCS src/ks_postgresql.cpp)
add_executable(ks_postgresql ${ks_postgresql_SRCS} ${ks_endpoint_SRCS})
target_link_libraries(ks_postgresql ${PostgreSQL_LIBRARIES} ${ks_endpoint_LIBS})
