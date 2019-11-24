Supported schema
================

Kitchen Sync's goal is to synchronize data, so currently it only knows how to synchronize tables.  It does know how to synchronize the table schemas well enough to be able to synchronize the data, but synchronizing of views, triggers, and other schema features is not currently implemented.

To be efficiently synced, each table must have either an actual primary key, a unique key that is suitable for use as a primary key (all columns non-nullable), or all columns non-nullable and an index covering all columns.  Any other tables will just be cleared and repopulated like a normal reload.

For each table, before synchronizing the actual table data, Kitchen Sync tries to synchronize:
* the list of columns, by name and position
* the type of each column, including nullability, size/length, and scale (see below)
* the default value, default function, or associated sequence name for each column
* the secondary indexes on the table, including uniqueness constraint

Supported data types
--------------------

Kitchen Sync can synchronize data of any type when the database server at the two ends is the same, as long as it uses the same format for input and output of those values.

In order to support synchronization between _different_ database servers, Kitchen Sync parses the database type strings returned by the database server in order to make its own portable interpretation.  At the other end, this standardized representation can be mapped back into a type string that's suitable for the receiving database.  The parsed type definition is also used to figure out the appropriate serialization format for the actual data as it is transmitted between the two ends.

This means that we implement portable syncing for any types we know how to convert between different database servers, and pass through the non-portable encoding for everything else.

### Basic types

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
| `FLOAT` | `real` | |
| `DOUBLE` | `double precision` | |
| `DECIMAL(n, m)` | `numeric(n, m)` | PostgreSQL also allows `numeric` with no precision or scale specification; MySQL converts this to `numeric(10, 0)` |
| `DATE` | `date` | |
| `TIME` | `time without time zone` | |
| `DATETIME` | `timestamp without time zone` | |
| `JSON` | `json` | On MariaDB there is no separate `JSON` type, a `LONGTEXT` with a `JSON_VALID` `CHECK` constraint will be used instead |
| `ENUM` | (custom type) | For PostgreSQL, the matching enumerated type must exist already |

See also the spatial types list below.

### MySQL-specific types

The following MySQL-specific types are supported and mapped to the PostgreSQL equivalents shown:

| MySQL type | PostgreSQL type | Conversion notes |
| --- | --- | --- |
| `TINYBLOB` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `BLOB` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `MEDIUMBLOB` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `VARBINARY(n)` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `BINARY(n)` | `bytea` | Type promotion, PostgreSQL conversion supports all values |
| `TINYTEXT` | `text` | Type promotion, PostgreSQL conversion supports all values |
| `TEXT` | `text` | Type promotion, PostgreSQL conversion supports all values |
| `MEDIUMTEXT` | `text` | Type promotion, PostgreSQL conversion supports all values |
| `TINYINT` | `smallint` | Type promotion, PostgreSQL conversion supports all values |
| `MEDIUMINT` | `integer` | Type promotion, PostgreSQL conversion supports all values |
| `UNSIGNED TINYINT` | `smallint` | Type promotion, PostgreSQL conversion supports all values |
| `UNSIGNED SMALLINT` | `smallint` | PostgreSQL conversion will fail for unsigned values over 2^15 |
| `UNSIGNED MEDIUMINT` | `integer` | Type promotion, PostgreSQL conversion supports all values |
| `UNSIGNED INT` | `integer` | PostgreSQL conversion will fail for unsigned values over 2^31 |
| `UNSIGNED BIGINT` | `bigint` | PostgreSQL conversion will fail for unsigned values over 2^63 |

### PostgreSQL-specific types

The following PostgreSQL types are supported and mapped to the MySQL equivalents shown:

| PostgreSQL | MySQL type | Conversion notes |
| --- | --- | --- |
| `json` | `CHAR(36)` | A comment will be used to mark the field as JSON so that you can sync back the other way |

### Spatial types

The following spatial types are supported, and values are re-encoded for bi-directional portability between the databases, but for all spatial types PostgreSQL also supports additional extensions (eg. 3D coordinates) which are not supported by MySQL; SRID support also depends on database server version:

| PostgreSQL/PostGIS type | MySQL type | Conversion notes |
| --- | --- | --- |
| `geometry` | `GEOMETRY` | |
| `geometry(point)` | `POINT` | |
| `geometry(linestring)` | `LINESTRING` | |
| `geometry(polygon)` | `POLYGON` | |
| `geometry(geometrycollection)` | `GEOMETRYCOLLECTION` | |
| `geometry(multipoint)` | `MULTIPOINT` | |
| `geometry(multilinestring)` | `MULTILINESTRING` | |
| `geometry(multipolygon)` | `MULTIPOLYGON` | |
| `geography(geometry, x)` | `GEOMETRY SRID x` | |
| `geography(point, x)` | `POINT SRID x` | |
| `geography(linestring, x)` | `LINESTRING SRID x` | |
| `geography(polygon, x)` | `POLYGON SRID x` | |
| `geography(geometrycollection, x)` | `GEOMETRYCOLLECTION SRID x` | |
| `geography(multipoint, x)` | `MULTIPOINT SRID x` | |
| `geography(multilinestring, x)` | `MULTILINESTRING SRID x` | |
| `geography(multipolygon, x)` | `MULTIPOLYGON SRID x` | |
| `geometry(geometry, x)` | `GEOMETRY SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(point, x)` | `POINT SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(linestring, x)` | `LINESTRING SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(polygon, x)` | `POLYGON SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(geometrycollection, x)` | `GEOMETRYCOLLECTION SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(multipoint, x)` | `MULTIPOINT SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(multilinestring, x)` | `MULTILINESTRING SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |
| `geometry(multipolygon, x)` | `MULTIPOLYGON SRID x` | When syncing from MySQL to PostgreSQL the `geography` type will be expected instead, as above |

### Asking for help with unsupported types

Please note that as discussed above, we only need to add specific support for a type when there is actually a type we can usefully map it to on the other database server.

To discuss adding mappings for new types, please use GitHub issues and check if your issue has already been reported first.  Please describe the use case(s) you know of for the column type you are using, as it helps to determine the best equivalent type on the other database.

Schema things currently ignored
-------------------------------

* Character sets
* Collation orders
* Index types (except for uniqueness and spatial support)
* `ASC`/`DESC` flags in index columns
* Views
* Triggers
* Anything else not mentioned on this page!
