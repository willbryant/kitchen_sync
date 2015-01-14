Changelog
=========

0.28
----
* Drop extra columns (if `--alter` is used) without recreating the table.

0.26
----
* Fix dropping defaults on non-nullable columns on mysql.

0.25 - Christmas edition
----
* Reset sequences after syncing table rows on postgresql.

0.24
----
* Implement bytea encoding for postgresql.

0.23
----
* Implement auto-creation of postgresql sequences for serial columns.
* Numerous fixes to postgresql table creation and cross-compatibility with mysql.

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
* Implement the --alter option to automatically apply them.

0.18
----
* Protocol schema additions to prepare for 0.19: extract and check whether columns are `auto_increment`/`serial`.

0.17
----
* Give feedback to the user if protocol versions are incompatible
* Fix no default on boolean columns getting parsed as default false on mysql

0.16
----
* Docs now tell you to 'make install' rather than just 'make'
* Internal refactoring

0.15
----
* Extract and check default values on columns
* Fix a redefinition warning on yosemite's version of clang
* Parse tinyint(n != 1) and mediumint mysql column definitions for completeness
* Give more helpful output if tests fail due to an EOFError while reading a command
* Hopefully fix intermittent failures due to stderr output from the snapshot import/export test
* Use msgpack 0.5.9 gem for ruby tests

0.14
----
* Use YamlCPP from the system if already present and found by pkgconfig.  Thanks @proglottis.

0.13
----
* Fix URL decode calls, need to be from class scope.  Thanks Van for patch.

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
* Use `NO_WRITE_TO_BINLOG` in the mysql `FLUSH TABLES` so downstream slaves aren't affected.

0.5
---
* Use `execvp` instead of `execv` so `ks` can run from say `/usr/local/bin` without explicitly specifying the path.

0.4
---
* Decode postgresql bytea strings before hashing or sending in response to a rows command; decode booleans and pack as msgpack boolean type for portability.
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
* Use bundler to install the gems needed to run the Ruby tests.
* Implement column type and length extraction/serialization/checking, supporting the basic set of mostly-portable types.
* Numerous internal and protocol improvements.

0.1
---
* First release for testing.
