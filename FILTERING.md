# Filtering data

Kitchen Sync allows you to filter the data as it is read from the source database.  You can filter out rows, replace the values in columns, or filter out entire tables.

## Specifying filter options

First, make a YAML file, and specify it using the `--filter` option:

```
ks --from postgresql://someuser:mypassword@localhost/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb \
   --filter copy_prod_to_test.yml
```

The YAML file should contain a hash/map where the top level keys are the names of each table to filter; if you don't mention a table, it will be synchronized with no changes.  Under each table, you can use the following options.

### `clear`

This option filters out all rows, emptying the table at the target end.  For example:

```
background_tasks: clear
```

This option is shorthand for `only: false`.

### `only`

This option is used to give a boolean expression (written in the SQL dialect of your source database server) which will be applied to each row at the source end to determine whether it should be copied to the target end.  For example:

```
posts:
  only: published_at IS NOT NULL
```

Means that the posts table at the target end will receive only the rows from the source end that have the `published_at` field set.

Note that any other rows at the target end will be cleared, not left; Kitchen Sync only implements uni-directional synchronization and fully synchronizes each table, even if it's told to filter out some of the source rows.

### The `replace` option

This option is used to give a table of expressions (written in the SQL dialect of your source database server) to substitute for the actual column values.  Two points need to be kept in mind when using the `replace` option.

First, Kitchen Sync uses regular YAML files, so you may need to make quote your expressions to stop the YAML parser choking on them.  YAML supports both single quotes and double quotes for values.

Second, the values you give for each column are SQL _expressions_, not literals.  This means that they call SQL functions and operators, or simply refer to other columns.  For example:

```
users:
  replace:
    last_name: id
```

Would replace the `email` column values with the values of the `id` column, not with the literal string `'id'`.

So to express a literal value, you need to quote it, or the SQL server will try and parse it as a column reference or function call.  But the first point above about YAML quoting creates a problem - for example, if we try:

```
users:
  replace:
    last_name: 'Smith'
```

This won't work as those quotes around the literal value will be interpreted by the YAML parser, not the SQL server.  The SQL server will therefore be told to get the value of a column or function called `Smith`, not the literal string `'Smith'`.  You therefore usually need to double-quote any literal values:

```
users:
  replace:
    last_name: "'Smith'"
```

This is ugly, but necessary to preserve the flexibility to use both literals and expressions.  For example:

```
users:
  replace:
    email: "'user' || users.id || '@example.com'"
    last_name: "'Smith'"
    phone_number: 12345678
    password_hash: NULL
```

Finally, please note that using the `replace` option with the primary key column is not supported, and will cause errors.

### Combinining options

Although it doesn't make sense to combine the `clear` option with the other options, it is legal and common to combine the `only` and `replace` options on a single table.  For example:

```
posts:
  only: published_at IS NOT NULL

users:
  only: "terms_and_conditions_accepted AND NOT deactivated"
  replace:
    last_name: "'Smith'"
    password_hash: 'dummy' || MD5(users.id)

background_tasks: clear
```

## Syncing just a subset of tables

Another useful option is `--only` which you can use to specify the names of the tables to sync.  For example:

```
ks --from postgresql://someuser:mypassword@localhost/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb \
   --only table1,table2,table3
```

This option will leave the data in any other tables in your target database unchanged.

## Syncing all except a subset of tables

The `--ignore` option can be used to specify the names of tables _not_ to sync (ie. the opposite of `--only`).  For example:

```
ks --from postgresql://someuser:mypassword@localhost/sourcedb \
   --to postgresql://someuser:mypassword@localhost/targetdb \
   --ignore table4,table5
```

This option will leave the data in the given tables in your target database unchanged.
