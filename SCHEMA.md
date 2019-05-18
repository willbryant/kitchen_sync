Supported schema
================

Kitchen Sync currently only knows how to synchronize tables.  Views, triggers, and other schema features are not yet implemented, but pull requests are welcome.

Each table must have either an actual primary key, or a unique key that is suitable for use as a primary key (all columns non-nullable).

Kitchen Sync tries to synchronize:
* the list of columns, by name and position
* the type of each column, including nullability, size/length, and scale (see below)
* the default value, default function, or associated sequence name for each column
* the secondary indexes on the table, including uniqueness constraint

Supported data types
--------------------

In order to support synchronization between different databases, Kitchen Sync parses the database type strings returned by the database server in order to make its own portable interpretation.  At the other end, this standardized representation can be mapped back into a type string that's suitable for the receiving database.  The parsed type definition is also used to figure out the appropriate serialization format for the actual data as it is transmitted between the two ends.

The downside of doing all this is that only those types that we have set up a mapping for are currently supported, and so far only types that have an exact or "close enough" equivalent on both MySQL and PostgreSQL have been implemented.

Currently the following fully-compatible standard types are supported:

| MySQL type | PostgreSQL type | Conversion notes |
| --- | --- | --- |
| `LONGBLOB` | `bytea` | |
| `LONGTEXT` | `text` | |
| `VARCHAR(n)` | `character varying(n)` | |
| `CHAR(n)` | `character(n`) | |
| `TINYINT(1)` | `boolean` | |
| `SMALLINT` | `smallint` | |
| `INT` | `integer` | |
| `BIGINT` | `bigint` | |
| `UNSIGNED SMALLINT` | `smallint` | PostgreSQL conversion will fail for unsigned values over 2^15 |
| `UNSIGNED INT` | `integer` | PostgreSQL conversion will fail for unsigned values over 2^31 |
| `UNSIGNED BIGINT` | `bigint` | PostgreSQL conversion will fail for unsigned values over 2^63 |
| `FLOAT` | `real` | |
| `DOUBLE` | `double precision` | |
| `DECIMAL(n, m`) | `numeric(n, m)` | PostgreSQL also allows `numeric` with no precision or scale specification; MySQL converts this to `numeric(10, 0)` |
| `DATE` | `date` | |
| `TIME` | `time without time zone` | |
| `DATETIME` | `timestamp without time zone` | |

And the following MySQL types are supported and mapped to the PostgreSQL equivalents shown:

| MySQL type | PostgreSQL type | Conversion notes |
| --- | --- | --- |
| `TINYBLOB` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `BLOB` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `MEDIUMBLOB` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `TINYTEXT` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `TEXT` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `MEDIUMTEXT` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `TINYINT` | `smallint` | Type promotion, PostgreSQL conversion supports all values |
| `MEDIUMINT` | `integer` | Type promotion, PostgreSQL conversion supports all values |
| `UNSIGNED TINYINT` | `smallint` | Type promotion, PostgreSQL conversion supports all values |
| `UNSIGNED MEDIUMINT` | `integer` | Type promotion, PostgreSQL conversion supports all values |

If your tables use a type not in the table above, please use Github issues, and check if your issue has already been reported first.  It is helpful if you can describe the use case for the column type you are using as it helps to determine the best equivalent type on the other database.

Schema things currently ignored
-------------------------------

* Character sets
* Collation orders
* Index types (except for uniqueness)
* `ASC`/`DESC` flags in index columns
* Views
* Triggers
* Anything else not mentioned on this page!
