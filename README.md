Kitchen Sync
============

Goal: Fast unidirectional synchronization - make or efficiently update a copy of a database, without slow dumping & reloading.

Kitchen Sync aims to:
* **Finish the job much faster than full reloads**: working in parallel and only modifying the data that's actually changed, Kitchen Sync is usually 5-20 times faster than loading from dumps, which minimizes the length of time you need to take our test environments down to resync and helps bring up new developer and test environments quickly.
* **Work efficiently over Internet or WAN links**: Kitchen Sync can run on separate servers at either end of a long connection, using an internal protocol that's optimised to work well over long links, making it possible to resynchronize database servers at different datacentres or laptops and test servers at remote offices and homes.
* **Transport over SSH**: Kitchen Sync has built-in support for transport over SSH connection(s), so no extra firewall permissions need to be added or extra access granted to protected servers to synchronize from remote database servers.
* **Synchronise between different database servers or versions**: Kitchen Sync performs logical synchronisation using the regular SQL database interface rather than synchronisation of the files on disk, so you can use it to synchronize to database servers with different architectures, operating systems, storage engines, major versions, compression options, and even competing database products (MySQL, MariaDB, and PostgreSQL currently supported).
* **Minimize write traffic to the target database**: as well as maximising update performance & SSD life, if you host your target database on a filesystem or storage cluster that supports Copy-on-Write, the storage requirements will grow only in proportion to the actual changes, you can store many versions of the database with minimal storage growth for datasets that tend to have large amounts of unchanged data between versions.
* **Produce partial replicas**: optionally exclude tables that are not required at the target end, or synchronise only specific tables or even only rows matching certain criteria, with all other data being cleared at the target end to help reset to a known state.
* **Filter out sensitive data on-the-fly**: define column expressions to overwrite certain data as it is retrieved from the source database server, to ensure that sensitive customer or business data never leaves the origin database server - even if a full-sized and otherwise complete production-like copy is needed for testing and development.
* **Check and update schema**: Kitchen Sync will check that the target database schema matches the source, and can (optionally) recreate or alter tables to make them match.

Installation
------------

Please see [Installing Kitchen Sync](INSTALL.md).

Usage
-----

Please see [Using Kitchen Sync](USAGE.md) to get started.

Supported databases
-------------------
* MySQL/Percona Server/MariaDB (5.5 and above)
* PostgreSQL (9.2 and above)

Bugs
----

Please use Github issues and check if your issue has already been reported first.
