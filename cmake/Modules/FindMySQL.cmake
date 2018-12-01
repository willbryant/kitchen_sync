# - Find mysql client library
# Find the native MySQL includes and library
#
# You may manually set (if not found automatically):
#  MySQL_INCLUDE_DIR - Where to find mysql.h.
#  MySQL_LIBRARY_DIR - Where to find the corresponding library.
#
# The following variables are set if the mysql client library is found:
#  MySQL_INCLUDE_DIR - Where to find mysql.h.
#  MySQL_LIBRARIES   - List of libraries when using MySQL.
#  MySQL_FOUND       - True if MySQL found.

FIND_PATH(MySQL_INCLUDE_DIR mysql.h
  /usr/local/include/mysql
  /usr/include/mysql
  /usr/include/mariadb
  /usr/local/include/mariadb
)

SET(MySQL_NAMES mysqlclient mysqlclient_r mariadbclient mariadbclient_r)
IF(MySQL_LIBRARY_DIR)
  FIND_LIBRARY(MySQL_LIBRARY
    NAMES ${MySQL_NAMES}
    PATHS ${MySQL_LIBRARY_DIR}
    NO_DEFAULT_PATH
  )
ELSE()
  FIND_LIBRARY(MySQL_LIBRARY
    NAMES ${MySQL_NAMES}
    PATHS /usr/lib /usr/local/lib
    PATH_SUFFIXES mysql
  )
  get_filename_component(MySQL_LIBRARY_DIR ${MySQL_LIBRARY} PATH)
ENDIF()

IF (MySQL_INCLUDE_DIR AND MySQL_LIBRARY)
  SET(MySQL_FOUND TRUE)
  SET( MySQL_LIBRARIES ${MySQL_LIBRARY} )
ELSE (MySQL_INCLUDE_DIR AND MySQL_LIBRARY)
  SET(MySQL_FOUND FALSE)
  SET( MySQL_LIBRARIES )
ENDIF (MySQL_INCLUDE_DIR AND MySQL_LIBRARY)

IF (MySQL_FOUND)
  MESSAGE(STATUS "Found MySQL: ${MySQL_LIBRARY} ${MySQL_INCLUDE_DIR}")
ELSE (MySQL_FOUND)
  IF (MySQL_FIND_REQUIRED)
    MESSAGE(STATUS "Looked for MySQL libraries named ${MySQL_NAMES}.")
    MESSAGE(FATAL_ERROR "Could NOT find MySQL library")
  ENDIF (MySQL_FIND_REQUIRED)
ENDIF (MySQL_FOUND)

MARK_AS_ADVANCED(
  MySQL_LIBRARY
  MySQL_INCLUDE_DIR
)
