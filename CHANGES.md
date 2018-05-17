Changelog
=========

1.0
---
* Behavior change: when using `--via` and the `--filter` option, the filter file is read locally rather than at the `--via` end.

0.99.2
------
Third release candidate for 1.0
* Only `--debug` logging will output timestamps, not `--verbose`
* Forward compatibility with forthcoming behavior changes to filtering
* Forward compatibility with later msgpack versions and with setting up the test suite with later versions of mysql

0.99.1
------
Second release candidate for 1.0
* Fix algorithm problem in a special case involving unique keys and the last row of the table being replaced by a later one
* Tune some block size numbers

0.99
----
Release candidate for 1.0
* Rewrite protocol and sync algorithm to hash concurrently at the two ends, and pipeline commands where possible.

0.60
----
* Don't complain about unsupported column types on ignored tables or tables that are going to be dropped anyway.
* Reduce max block size in order to reduce rework when mismatches with certain patterns are detected.
* Drop support for the protocol used by version 0.45 and earlier.

0.59
----
* Initial support for MySQL's special `timestamp` type and for `DEFAULT CURRENT_TIMESTAMP` and `ON UPDATE CURRENT_TIMESTAMP` behaviors on `timestamp` and `datetime` columns.
* Support PostgreSQL's `time with time zone` and `timestamp with time zone` column types.
* Fix support for PostgreSQL's default expressions.

0.58
----
* When using `--alter`, change the order in which keys are dropped, to work around https://bugs.mysql.com/bug.php?id=57497.

0.57
----
* Change default SSH cipher to aes256-ctr for compatibility with more platforms.  Thanks @soundasleep and @aclemons.
* Don't complain about tables that are missing a primary key (or suitable unique key) if they are ignored.
* Stop printing debugging backtraces on SQL errors.
* Document schema support.

0.56
----
* Fixes to compilation on OpenBSD and FreeBSD.  Thanks @aclemons.

0.55
----
* Interpret `postgres://` URLs to mean `postgresql://` for compatibility.  Thanks @aclemons.

0.54
----
* Add an option to change the SSH cipher, and default to aes-256-gcm for compatibility with OpenSSH 7.2 and better speed.  Thanks @normanv.

0.53
----
* Look for MariaDB client libraries in mariadb-specific directories as well for compatibility with Ubuntu 16.04.  Thanks @normanv.
* Declare license (MIT).

0.52
----
* Look for MariaDB client libraries as well.
* Enhanced readme.

0.51
----
* Add `--progress` option to print progress dots, which used to come on automatically with `--verbose` mode, not always useful in scripts.
* Add progress dots during the long series of inserts at the end of a table.

0.50
----
* Add `--structure-only` option, usually used with `--alter` to populate a database's schema.  Thanks @dylanmckay.

0.49
----
* Avoid PostgreSQL 9.3 syntax to add support for 9.2 (and partial support for earlier versions).

0.48
----
* Work around CMake not finding PostgreSQL unless the server development header files are present.

0.47
----
* Improve error messages from exceptions.
* Start converting the endpoints to take their startup info from the environment rather than the command line.

0.46
----
* Add a simple progress meter with `--verbose`.  Thanks @soundasleep.
* Implement optional xxHash64 support.
* Add an option to override the path the 'from' binaries are in when using `--via`.

0.45
----
* Fix pthreads link problems on newer Linux distros.

0.44
----
* Fix `ks_mysql` or `ks_postgresql` not being found on some installs because PATH was not used in all cases.

0.43
----
* Fix `--debug` listing arguments it wouldn't actually use if `--via` was not given.
* Fixes URL examples in usage documentation.

0.42
----
* Limit the maximum block size for a single hash command to reduce rework.
* Optimisations from testing of the 0.40 and 0.41 changes.

0.41
----
* Fix regression in 0.40 where a DELETE query could be run while an unbufferred SELECT query was run on MySQL.

0.40
----
* Better algorithm for applying row changes that avoids using large amounts of memory when the 'from' end has deleted a large set of rows.
* Default `long_query_time` to a higher value on MySQL to reduce annoying noise in logs.

0.39
----
* Better forward compatibility with future versions that may add new entries to the database schema definitions.
* Look for PostgreSQL version 9.5 too, even if not included in your platform's cmake module.
* Fix various compiler warnings reported by users.

0.38
----
* Remove unnecessary OpenSSL dependency on OS X (we use Common Crypto since 0.2).

0.37
----
* Build configuration changes to fix using a system-wide copy of yaml-cpp.  Thanks @pvenegas.

0.36
----
* Don't attempt to add non-nullable columns or make nullable columns non-nullable if there are any unique keys defined on the columns, as this will inevitably hit duplicate key errors.
* Fix MySQL 5.6.5+ compatibility.  Thanks @bagedevimo.

0.35
----
* Add new columns (if `--alter` is used) to the end of the columns list without recreating the table.

0.34
----
* Replace the `--partial` and `--rollback-after` option with a more general `--commit` option and add options to commit after each table or insert batch.  `--commit often` is useful to avoid large rollback segments on MySQL.
* Fix a potential invalid memory access in the case where table columns don't match.
* Boost's program_options library is no longer a dependency.  Boost is still used at compile time.

0.32
----
* Send quit commands to the 'from' endpoints as soon as possible.

0.31
----
* Change column nullability (if `--alter` is used) without recreating the table.

0.30
----
* Look for PostgreSQL version 9.4 too, even if not included in your platform's cmake module.
* Fix dropping multiple columns on a table in one pass.
* Debugging: comment briefly on what part of tables doesn't match.

0.29
----
* Debugging: dump the worker arguments if run with `--debug`.

0.28
----
* Drop extra columns (if `--alter` is used) without recreating the table.

0.26
----
* Fix dropping defaults on non-nullable columns on MySQL.

0.25 - Christmas edition
----
* Reset sequences after syncing table rows on PostgreSQL.

0.24
----
* Implement `bytea` encoding for PostgreSQL.

0.23
----
* Implement auto-creation of PostgreSQL sequences for serial columns.
* Numerous fixes to PostgreSQL table creation and cross-compatibility with MySQL.

0.22
----
* Set or clear column defaults (if `--alter` is used) without recreating the entire table.
* Add missing include to fix GCC compilation errors.

0.21
----
* Add or remove keys (if `--alter` is used) without recreating the entire table.

0.20
----
* Documentation.  Moar documentation.

0.19
----
* Automatically figure out what tables need to be restructured to make the database schema match.
* Implement the `--alter` option to automatically apply them.

0.18
----
* Protocol schema additions to prepare for 0.19: extract and check whether columns are `auto_increment`/`serial`.

0.17
----
* Give feedback to the user if protocol versions are incompatible
* Fix no default on boolean columns getting parsed as default false on MySQL

0.16
----
* Docs now tell you to `make install` rather than just `make`
* Internal refactoring

0.15
----
* Extract and check default values on columns
* Fix a redefinition warning on yosemite's version of clang
* Parse `tinyint(n != 1)` and `mediumint` MySQL column definitions for completeness
* Give more helpful output if tests fail due to an `EOFError` while reading a command
* Hopefully fix intermittent failures due to stderr output from the snapshot import/export test
* Use msgpack 0.5.9 gem for ruby tests

0.14
----
* Use YamlCPP from the system if already present and found by pkgconfig.  Thanks @proglottis.

0.13
----
* Fix URL decode calls, need to be from class scope.  Thanks @pvenegas for patch.

0.12
----
* URL-decode the parts of the database URLs before using them.

0.11
----
* Implement `--set-from-variables` and `--set-to-variables` options.

0.10
----
* When sending rows, limit the queries to 10000 rows at once, to reduce slow query warnings and buffering.

0.9
---
* Substantially reduce `to_descriptor_list_start`, as it turns out the OS X default `getdtablesize()` is a mere 256.

0.8
---
* Fix filter_file argument getting trampled, use strings everywhere so the argvs have definitely been copied out before any status messages are written.

0.7
---
* Set the current table name as the `argv[1..]` of the from endpoints so admins can see what they're doing.

0.6
---
* Use `NO_WRITE_TO_BINLOG` in the MySQL `FLUSH TABLES` so downstream slaves aren't affected.

0.5
---
* Use `execvp` instead of `execv` so `ks` can run from say `/usr/local/bin` without explicitly specifying the path.

0.4
---
* Decode PostgreSQL `bytea` strings before hashing or sending in response to a rows command; decode booleans and pack as msgpack boolean type for portability.
* Improvements to the protocol packing format and internal improvements to memory formats.

0.3
---
* Add install recipe.
* First version used in anger by Powershop.

0.2
---
* GCC compatibility fixes.
* Use Common Crypto instead of OpenSSL on OS X to fix deprecation warnings.
* Use the database-specific quoting methods instead of a hardcoded standard version.
* Use Bundler to install the gems needed to run the Ruby test suite.
* Implement column type and length extraction/serialization/checking, supporting the basic set of mostly-portable types.
* Numerous internal and protocol improvements.

0.1
---
* First release for testing.
