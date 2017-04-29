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

In order to support synchronization between different databases, Kitchen Sync maps the database type strings returned by the database server to a standardized enumerated type.  At the other end, this standardized type can be mapped back into a type string suitable for the receiving database.  The enumerated type is also used to determine the appropriate serialization format for the row tuple values as they are being transmitted between the two ends.

The downside of doing this is that only those types that we have set up a mapping for are supported, and so far only types that have an exact or "close enough" equivalent on both MySQL and PostgreSQL have been implemented.

Currently the following fully-compatible standard types are supported:

| KS type | MySQL type | PostgreSQL type | Conversion notes |
| --- | --- | --- | --- |
| BLOB | LONGBLOB | bytea | |
| TEXT | LONGTEXT | text | |
| VCHR (size n) | VARCHAR(n) | character varying(n) | |
| FCHR (size n) | CHAR(n) | character(n) | |
| BOOL | TINYINT(1) | boolean | | 
| SINT (size 2) | SMALLINT | smallint | |
| SINT (size 4) | INT | integer | |
| SINT (size 8) | BIGINT | bigint | |
| UINT (size 2) | UNSIGNED SMALLINT | smallint | PostgreSQL conversion will fail for unsigned values over 2^15 |
| UINT (size 4) | UNSIGNED INT | integer | PostgreSQL conversion will fail for unsigned values over 2^31 |
| UINT (size 8) | UNSIGNED BIGINT | bigint | PostgreSQL conversion will fail for unsigned values over 2^63 |
| REAL (size 4) | FLOAT | real | |
| REAL (size 8) | DOUBLE | double precision | |
| DECI (size n, scale m) | DECIMAL(n, m) | numeric(n, m) | |
| DATE | DATE | date | | 
| TIME | TIME | time without time zone | |
| DTTM | DATETIME | timestamp without time zone | |

And the following MySQL types are supported and mapped to the PostgreSQL equivalents shown:

| KS type | MySQL type | PostgreSQL type | Conversion notes |
| --- | --- | --- | --- |
| BLOB (size 1) | TINYBLOB | bytea | Type promotion, PostgreSQL conversion supports all values |
| BLOB (size 2) | BLOB | bytea | Type promotion, PostgreSQL conversion supports all values |
| BLOB (size 3) | MEDIUMBLOB | bytea | Type promotion, PostgreSQL conversion supports all values |
| TEXT (size 1) | TINYTEXT | bytea | Type promotion, PostgreSQL conversion supports all values |
| TEXT (size 2) | TEXT | bytea | Type promotion, PostgreSQL conversion supports all values |
| TEXT (size 3) | MEDIUMTEXT | bytea | Type promotion, PostgreSQL conversion supports all values |
| SINT (size 1) | TINYINT | smallint | Type promotion, PostgreSQL conversion supports all values |
| SINT (size 3) | MEDIUMINT | integer | Type promotion, PostgreSQL conversion supports all values |
| UINT (size 1) | UNSIGNED TINYINT | smallint | Type promotion, PostgreSQL conversion supports all values |
| UINT (size 3) | UNSIGNED MEDIUMINT | integer | Type promotion, PostgreSQL conversion supports all values |

If your tables use a type not in the table above, please use Github issues, and check if your issue has already been reported first.  It is helpful if you can describe the use case for the column type you are using as it helps to determine the best equivalent type on the other database.

Schema things currently ignored
-------------------------------

* Character sets
* Collation orders
* Index types (except for uniqueness)
* ASC/DESC flags in index columns
* Views
* Triggers
* Anything else not mentioned on this page!
