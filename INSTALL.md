Installing Kitchen Sync
=======================

To compile Kitchen Sync, you will need:
* a C++14 compiler
* CMake
* OpenSSL library headers (except on macOS, where Apple's Common Crypto library is used instead)
* PostgreSQL client library headers; and/or
* MySQL or MariaDB client library headers

(See the 'Compiling in support for different databases' section at the bottom for more details about how this works.)

Ubuntu
------

You can install the above build dependencies on Ubuntu using:
```
apt-get install build-essential cmake libssl-dev
```

And one or both of:
```
apt-get install libpq-dev
apt-get install libmysqlclient-dev
```

You can use MariaDB's libraries instead of MySQL's if you prefer:

```
apt-get install libmariadbclient-dev-compat
```

To build, change to the kitchen_sync directory where you checked out the files, then:
```
  cd build
  cmake .. && make && make install
```

CentOS 7
----------

You can install the above build dependencies on CentOS 7 using:
```
yum install gcc gcc-c++ make cmake openssl-devel
```

And one or both of:
```
yum install postgresql-devel
yum install mariadb-devel
```

MariaDB is now the default replacement for mysql on CentOS.

To build, change to the kitchen_sync directory where you checked out the files, then:
```
  cd build
  cmake .. && make && make install
```


macOS
-----

Kitchen Sync is now available in Homebrew.  You can install it using:
```
brew install kitchen-sync
```

To compile from source yourself, Homebrew users can install the above build dependencies using:
```
brew install cmake
```

And one or both of:
```
brew install postgresql
brew install mysql
```

You can use MariaDB instead of MySQL if you prefer:

```
brew install mariadb
```

To build, change to the kitchen_sync directory where you checked out the files, then:
```
  cd build
  cmake .. && make && make install
```

Next steps
----------

Please see [Using Kitchen Sync](USAGE.md) to get started.

If you'd like to check everything is working first, or submit patches to Kitchen Sync, please see [Running the test suite](TESTS.md).

Compiling in support for different databases
--------------------------------------------

Kitchen Sync needs at least one database client library to do something useful, but it will produce a separate binary for each different database, so you don't need to compile or deploy all the binaries on systems where you won't use them.

To support this, Kitchen Sync will automatically look for those database client libraries when you run `cmake`, and compile just the applicable binaries.  This means that you only need to install the client libraries that are relevant to you in order to compile Kitchen Sync.

If it can't find any out of the postgresql, mysql, or mariadb client libraries, by default the build will stop, because this is probably going to produce a useless build of Kitchen Sync that can't actually talk to any databases.

If you see that error, please check that you have the database client C libraries and their header files installed, as not all server distributions include the client library files in the same package, and not all client library distributions include the actual header files you need to compile a new program against the library in the same package.  The instructions below install the "dev" or "devel" packages so that you get these header files.

Kitchen Sync fully supports MariaDB using its 'mysql' target, and it detects MariaDB-specific features based on the server version, not the client library that you compile against.  Because MariaDB's client library still acts as if it's the MySQL client library (right down to the names of the header files), you can't build against both in a single build - you have to pick one.  But because both projects are maintaining backwards compatibility with server/protocol versions from before they forked, builds compiled against the MySQL client library should generally work against MariaDB, and vice versa, so use whichever suits you best.
