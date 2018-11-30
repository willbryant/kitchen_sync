Installing Kitchen Sync
=======================

To compile Kitchen Sync, you will need:
* a C++14 compiler
* cmake
* boost headers
* openssl library headers (except on OS X where Apple's Common Crypto library is used instead)
* postgresql client library headers; and/or
* mysql (or mariadb) client library headers

Kitchen Sync needs at least one database client library to do something useful, but it will produce a separate binary for each different database, so you don't need to compile or deploy all the binaries on systems where you won't use them.

To support this, Kitchen Sync will automatically look for those database client libraries and compile the appropriate binaries.  If it can't find any, by default the build will stop, because this is probably going to produce a useless build of Kitchen Sync that can't actually talk to any databases; please check that you have the database client C libraries (and their header files) installed.

Ubuntu
------

You can install the above build dependencies on Ubuntu using:
```
apt-get install build-essential cmake libboost-dev libssl-dev
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

OS X
----

Kitchen Sync is now available in Homebrew.  You can install it using:
```
brew install kitchen-sync
```

To compile from source yourself, Homebrew users can install the above build dependencies using:
```
brew install cmake boost
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
