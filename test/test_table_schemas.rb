# wrapper class used to help tests deal with some versions of PostgreSQL returning CURRENT_USER and CURRENT_SCHEMA and some using current_user and current_schema
class CaseInsensitiveString < String
  def ==(other)
    return super(other) || downcase == other
  end
end

module TestTableSchemas
  def create_footbl
    execute(<<-SQL)
      CREATE TABLE footbl (
        col1 INT NOT NULL,
        another_col SMALLINT,
        col3 VARCHAR(10),
        PRIMARY KEY(col1))
SQL
  end

  def footbl_def
    { "name"    => "footbl",
      "columns" => [
        {"name" => "col1",        "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "another_col", "column_type" => ColumnType::SINT_16BIT},
        {"name" => "col3",        "column_type" => ColumnType::TEXT_VARCHAR, "size" => 10}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def footbl_def_v7
    { "name"    => "footbl",
      "columns" => [
        {"name" => "col1",        "column_type" => LegacyColumnType::SINT, "size" => 4, "nullable" => false},
        {"name" => "another_col", "column_type" => LegacyColumnType::SINT, "size" => 2},
        {"name" => "col3",        "column_type" => LegacyColumnType::VCHR, "size" => 10}],
      "primary_key_type" => 1, # legacy value for explicit_primary_key
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_secondtbl
    execute(<<-SQL)
      CREATE TABLE secondtbl (
        tri BIGINT,
        pri1 INT NOT NULL,
        pri2 CHAR(2) NOT NULL,
        sec INT,
        PRIMARY KEY(pri2, pri1))
SQL
    execute(<<-SQL)
      CREATE INDEX secidx ON secondtbl (sec)
SQL
  end

  def secondtbl_def
    { "name"    => "secondtbl",
      "columns" => [
        {"name" => "tri",  "column_type" => ColumnType::SINT_64BIT},
        {"name" => "pri1", "column_type" => ColumnType::SINT_32BIT,              "nullable" => false},
        {"name" => "pri2", "column_type" => ColumnType::TEXT_FIXED, "size" => 2, "nullable" => false},
        {"name" => "sec",  "column_type" => ColumnType::SINT_32BIT}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [2, 1], # note order is that listed in the key, not the index of the column in the table
      "keys" => [
        {"name" => "secidx", "columns" => [3]}] }
  end

  def create_uniquetbl
    execute(<<-SQL)
      CREATE TABLE uniquetbl (
        pri INT NOT NULL,
        sec INT,
        col3 VARCHAR(1000),
        PRIMARY KEY(pri))
SQL
    execute(<<-SQL)
      CREATE UNIQUE INDEX secidx ON uniquetbl (sec)
SQL
  end

  def uniquetbl_def
    { "name"    => "uniquetbl",
      "columns" => [
        {"name" => "pri",  "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "sec",  "column_type" => ColumnType::SINT_32BIT},
        {"name" => "col3", "column_type" => ColumnType::TEXT_VARCHAR, "size" => 1000}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0], # note order is that listed in the key, not the index of the column in the table
      "keys" => [
        {"name" => "secidx", "unique" => true, "columns" => [1]}] }
  end

  def create_middletbl
    execute(<<-SQL)
      CREATE TABLE middletbl (
        pri INT NOT NULL,
        PRIMARY KEY(pri))
SQL
  end

  def middletbl_def
    { "name"    => "middletbl",
      "columns" => [
        {"name" => "pri", "column_type" => ColumnType::SINT_32BIT, "nullable" => false}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_texttbl
    execute(<<-SQL)
      CREATE TABLE texttbl (
        pri INT NOT NULL,
        textfield #{connection.text_column_type},
        PRIMARY KEY(pri))
SQL
  end

  def texttbl_def
    { "name"    => "texttbl",
      "columns" => [
        {"name" => "pri",       "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "textfield", "column_type" => ColumnType::TEXT}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_empty_misctbl
    connection.create_enum_column_type
    execute(<<-SQL)
      CREATE TABLE misctbl (
        pri INT NOT NULL,
        PRIMARY KEY(pri))
SQL
  end

  def create_misctbl
    connection.create_enum_column_type
    execute(<<-SQL)
      CREATE TABLE misctbl (
        pri INT NOT NULL,
        boolfield BOOL,
        datefield DATE,
        timefield #{connection.time_column_type},
        datetimefield #{connection.datetime_column_type},
        smallfield SMALLINT,
        floatfield #{connection.real_column_type},
        doublefield DOUBLE PRECISION,
        decimalfield DECIMAL(10, 4),
        vchrfield VARCHAR(9),
        fchrfield CHAR(9),
        uuidfield #{connection.uuid_column_type},
        textfield #{connection.text_column_type},
        blobfield #{connection.blob_column_type},
        jsonfield #{connection.json_column_type 'jsonfield'},
        enumfield #{connection.enum_column_type},
        PRIMARY KEY(pri))
SQL
  end

  def misctbl_def
    { "name"    => "misctbl",
      "columns" => [
        {"name" => "pri",           "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "boolfield",     "column_type" => ColumnType::BOOLEAN},
        {"name" => "datefield",     "column_type" => ColumnType::DATE},
        {"name" => "timefield",     "column_type" => ColumnType::TIME}.merge(connection.time_precision),
        {"name" => "datetimefield", "column_type" => ColumnType::DATETIME}.merge(connection.time_precision),
        {"name" => "smallfield",    "column_type" => ColumnType::SINT_16BIT},
        {"name" => "floatfield",    "column_type" => ColumnType::FLOAT_32BIT},
        {"name" => "doublefield",   "column_type" => ColumnType::FLOAT_64BIT},
        {"name" => "decimalfield",  "column_type" => ColumnType::DECIMAL, "size" => 10, "scale" => 4},
        {"name" => "vchrfield",     "column_type" => ColumnType::TEXT_VARCHAR, "size" => 9},
        {"name" => "fchrfield",     "column_type" => ColumnType::TEXT_FIXED, "size" => 9},
        {"name" => "uuidfield"}.merge(connection.uuid_column_type? ? {"column_type" => ColumnType::UUID} : {"column_type" => ColumnType::TEXT_FIXED, "size" => 36}),
        {"name" => "textfield",     "column_type" => ColumnType::TEXT},
        {"name" => "blobfield",     "column_type" => ColumnType::BINARY},
        {"name" => "jsonfield",     "column_type" => connection.json_column_type? ? ColumnType::JSON : ColumnType::TEXT},
        {"name" => "enumfield",     "column_type" => ColumnType::ENUMERATION, "enumeration_values" => misctbl_enum_column_values}.merge(connection.enum_column_subtype),
      ],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def misctbl_enum_column_values
    ["red", "green", "blue", "with'quote"]
  end

  def create_noprimarytbl(create_suitable_keys: true)
    execute(<<-SQL)
      CREATE TABLE noprimarytbl (
        nullable INT,
        version VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        non_nullable INT NOT NULL)
SQL
    execute "CREATE INDEX everything_key ON noprimarytbl (name, nullable, version, non_nullable)"
    execute "CREATE UNIQUE INDEX ignored_key ON noprimarytbl (nullable, version)"
    execute "CREATE UNIQUE INDEX correct_key ON noprimarytbl (version)" if create_suitable_keys
    execute "CREATE UNIQUE INDEX version_and_name_key ON noprimarytbl (version, name)"
    execute "CREATE UNIQUE INDEX non_nullable_key ON noprimarytbl (non_nullable)" if create_suitable_keys
    execute "CREATE INDEX not_unique_key ON noprimarytbl (non_nullable)"
  end

  def noprimarytbl_def(create_suitable_keys: true)
    { "name" => "noprimarytbl",
      "columns" => [
        {"name" => "nullable",     "column_type" => ColumnType::SINT_32BIT},
        {"name" => "version",      "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255, "nullable" => false},
        {"name" => "name",         "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255},
        {"name" => "non_nullable", "column_type" => ColumnType::SINT_32BIT, "nullable" => false}],
      "primary_key_columns" => (create_suitable_keys ? [1] : []),
      "primary_key_type" => (create_suitable_keys ? PrimaryKeyType::SUITABLE_UNIQUE_KEY : PrimaryKeyType::NO_AVAILABLE_KEY),
      "keys" => [ # sorted in uniqueness then alphabetic name order, but otherwise a transcription of the above create index statements
        ({"name" => "correct_key",          "key_type" => "unique", "columns" => [1]} if create_suitable_keys),
        {"name" => "ignored_key",          "key_type" => "unique", "columns" => [0, 1]},
        ({"name" => "non_nullable_key",     "key_type" => "unique", "columns" => [3]} if create_suitable_keys),
        {"name" => "version_and_name_key", "key_type" => "unique", "columns" => [1, 2]},
        {"name" => "everything_key",                               "columns" => [2, 0, 1, 3]},
        {"name" => "not_unique_key",                               "columns" => [3]}
      ].compact }
  end

  def noprimarytbl_def_v7(create_suitable_keys: true)
    { "name" => "noprimarytbl",
      "columns" => [
        {"name" => "nullable",     "column_type" => LegacyColumnType::SINT, "size" =>   4},
        {"name" => "version",      "column_type" => LegacyColumnType::VCHR, "size" => 255, "nullable" => false},
        {"name" => "name",         "column_type" => LegacyColumnType::VCHR, "size" => 255},
        {"name" => "non_nullable", "column_type" => LegacyColumnType::SINT, "size" =>   4, "nullable" => false}],
      "primary_key_columns" => (create_suitable_keys ? [1] : []),
      "primary_key_type" => (create_suitable_keys ? 2 : 0), # legacy values
      "keys" => [ # sorted in uniqueness then alphabetic name order, but otherwise a transcription of the above create index statements
        ({"name" => "correct_key",          "unique" => true,  "columns" => [1]} if create_suitable_keys),
        {"name" => "ignored_key",          "unique" => true,  "columns" => [0, 1]},
        ({"name" => "non_nullable_key",     "unique" => true,  "columns" => [3]} if create_suitable_keys),
        {"name" => "version_and_name_key", "unique" => true,  "columns" => [1, 2]},
        {"name" => "everything_key",       "unique" => false, "columns" => [2, 0, 1, 3]},
        {"name" => "not_unique_key",       "unique" => false, "columns" => [3]}
      ].compact }
  end

  def create_noprimaryjointbl(create_keys: true)
    execute(<<-SQL)
      CREATE TABLE noprimaryjointbl (
        table1_id INT NOT NULL,
        table2_id INT NOT NULL)
SQL
    execute "CREATE INDEX index_by_table1_id ON noprimaryjointbl (table1_id)" if create_keys
    execute "CREATE INDEX index_by_table2_id ON noprimaryjointbl (table2_id, table1_id)" if create_keys
  end

  def noprimaryjointbl_def(create_keys: true)
    { "name" => "noprimaryjointbl",
      "columns" => [
        {"name" => "table1_id", "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "table2_id", "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        ({"name" => "created_at", "column_type" => ColumnType::DATETIME, "nullable" => false} if create_keys == :partial)].compact,
      "primary_key_columns" => create_keys ? [1, 0] : [], # if index_by_table2_id exists, it will be chosen, and it happens to have the reverse order
      "primary_key_type" => PrimaryKeyType::ENTIRE_ROW_AS_KEY,
      "keys" => create_keys ? [
        {"name" => "index_by_table1_id", "columns" => [0]},
        {"name" => "index_by_table2_id", "columns" => [1, 0]}] : [] }
  end

  def create_some_tables
    clear_schema
    create_footbl
    create_secondtbl
  end

  def some_table_defs
    [footbl_def, secondtbl_def]
  end

  def from_keytbl_def
    { "name"    => "keytbl",
      "columns" => [
        {"name" => "pri",  "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "a",  "column_type" => ColumnType::SINT_32BIT},
        {"name" => "b", "column_type" => ColumnType::SINT_32BIT}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0], # note order is that listed in the key, not the index of the column in the table
      "keys" => [
        {"name" => "bidx", "unique" => true, "columns" => [2]}, # note the unique index sorts lexicographically after the standard index
		{"name" => "aidx", "unique" => false, "columns" => [1]}] }
  end

  def create_to_keytbl
	# The same table as from_keytbl_def but missing the unique index.
    execute(<<-SQL)
      CREATE TABLE keytbl (
		pri INT NOT NULL,
        a INT,
		b INT,
		PRIMARY KEY (pri))
SQL

	execute(<<-SQL)
	  CREATE INDEX aidx ON keytbl (a)
SQL
  end

  def create_reservedtbl
    execute(<<-SQL)
      CREATE TABLE reservedtbl (
        col1 INT NOT NULL,
        #{connection.quote_ident 'int'} INT,
        #{connection.quote_ident 'varchar'} INT,
        PRIMARY KEY(col1))
SQL
  end

  def reservedtbl_def
    { "name"    => "reservedtbl",
      "columns" => [
        {"name" => "col1"},
        {"name" => "int"},
        {"name" => "varchar"}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_defaultstbl
    execute(<<-SQL)
      CREATE TABLE defaultstbl (
        pri INT NOT NULL,
        varcharfield VARCHAR(32) DEFAULT 'test \\\\ with '' escaping©',
        emptydefaultstr VARCHAR(255) DEFAULT '',
        spacedefaultstr VARCHAR(255) DEFAULT ' ',
        zerodefaultstr VARCHAR(255) DEFAULT '0',
        nodefaultstr VARCHAR(255),
        charfield CHAR(5) DEFAULT 'test ',
        falseboolfield BOOL DEFAULT FALSE,
        trueboolfield BOOL DEFAULT TRUE,
        datefield DATE DEFAULT '2019-04-01',
        timefield #{connection.time_column_type} DEFAULT '12:34:56',
        datetimefield #{connection.datetime_column_type} DEFAULT '2019-04-01 12:34:56',
        #{"currentdatetimefield #{connection.datetime_column_type} DEFAULT CURRENT_TIMESTAMP#{"(6)" if connection.supports_fractional_seconds?}," if connection.supports_multiple_timestamp_columns?}
        floatfield #{connection.real_column_type} DEFAULT 42.625,
        doublefield DOUBLE PRECISION DEFAULT 0.0625,
        decimalfield DECIMAL(9, 3) DEFAULT '123456.789',
        PRIMARY KEY(pri))
SQL
  end

  def defaultstbl_def
    { "name"    => "defaultstbl",
      "columns" => [
        {"name" => "pri",                   "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "varcharfield",          "column_type" => ColumnType::TEXT_VARCHAR, "size" => 32,         "default_value" => "test \\ with ' escaping©"},
        {"name" => "emptydefaultstr",       "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255,        "default_value" => ""},
        {"name" => "spacedefaultstr",       "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255,        "default_value" => " "},
        {"name" => "zerodefaultstr",        "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255,        "default_value" => "0"},
        {"name" => "nodefaultstr",          "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255},
        {"name" => "charfield",             "column_type" => ColumnType::TEXT_FIXED,   "size" => 5,          "default_value" => "test "},
        {"name" => "falseboolfield",        "column_type" => ColumnType::BOOLEAN,                            "default_value" => "false"},
        {"name" => "trueboolfield",         "column_type" => ColumnType::BOOLEAN,                            "default_value" => "true"},
        {"name" => "datefield",             "column_type" => ColumnType::DATE,                               "default_value" => "2019-04-01"},
        {"name" => "timefield",             "column_type" => ColumnType::TIME,                               "default_value" => "12:34:56"}.merge(connection.time_precision),
        {"name" => "datetimefield",         "column_type" => ColumnType::DATETIME,                           "default_value" => "2019-04-01 12:34:56"}.merge(connection.time_precision),
        ({"name" => "currentdatetimefield", "column_type" => ColumnType::DATETIME, "default_expression" => "CURRENT_TIMESTAMP#{"(6)" if connection.supports_fractional_seconds?}"}.merge(connection.time_precision) if connection.supports_multiple_timestamp_columns?),
        {"name" => "floatfield",            "column_type" => ColumnType::FLOAT_32BIT,                        "default_value" => "42.625"},
        {"name" => "doublefield",           "column_type" => ColumnType::FLOAT_64BIT,                        "default_value" => "0.0625"},
        {"name" => "decimalfield",          "column_type" => ColumnType::DECIMAL, "size" => 9, "scale" => 3, "default_value" => "123456.789"}
      ].compact,
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_autotbl
    execute(<<-SQL)
      CREATE TABLE autotbl (
        inc #{connection.identity_column_type},
        payload INT NOT NULL,
        PRIMARY KEY(inc))
SQL
  end

  def autotbl_def
    { "name"    => "autotbl",
      "columns" => [
        {"name" => "inc",     "column_type" => ColumnType::SINT_32BIT, "nullable" => false, connection.identity_default_type => connection.identity_default_name("autotbl", "inc")},
        {"name" => "payload", "column_type" => ColumnType::SINT_32BIT, "nullable" => false}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_generatedtbl
    execute(<<-SQL)
      CREATE TABLE generatedtbl (
        pri INT,
        fore INT,
        stor INT GENERATED ALWAYS AS ((fore+1)*2) STORED,
        #{"virt INT GENERATED ALWAYS AS (fore*3) VIRTUAL," if connection.supports_virtual_generated_columns?}
        back INT,
        PRIMARY KEY(pri))
SQL
  end

  def generatedtbl_def
    { "name"    => "generatedtbl",
      "columns" => [
        {"name" => "pri",  "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "fore", "column_type" => ColumnType::SINT_32BIT},
        {"name" => "stor", "column_type" => ColumnType::SINT_32BIT, "generated_always_stored" => "((#{connection.quote_ident_for_generation_expression 'fore'} + 1) * 2)"},
        ({"name" => "virt", "column_type" => ColumnType::SINT_32BIT, "generated_always_virtual" => "(#{connection.quote_ident_for_generation_expression 'fore'} * 3)"} if connection.supports_virtual_generated_columns?),
        {"name" => "back", "column_type" => ColumnType::SINT_32BIT},
      ].compact,
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

def conflicting_indexed_table_def(suffix = "")
    { "name"    => "conflicting_indexed_table#{suffix}",
      "columns" => [
        {"name" => "id",  "column_type" => ColumnType::SINT_64BIT},
        {"name" => "sec", "column_type" => ColumnType::SINT_32BIT}
      ],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [
        {"name" => "index_on_second_column", "columns" => [1]},
        {"name" => "index_on_second_column_and_id_with_an_unnecessarily_long_key_name", "columns" => [1, 0]}
      ] }
  end

  def create_fkc_parenttbl
    execute(<<-SQL)
CREATE TABLE fkc_parenttbl (
  id INT NOT NULL,
  PRIMARY KEY(id))
SQL
  end

  def fkc_parenttbl_def
    { "name"    => "fkc_parenttbl",
      "columns" => [
        {"name" => "id", "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
      ],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_fkc_childtbl
    execute(<<-SQL)
CREATE TABLE fkc_childtbl (
  id INT NOT NULL,
  parent_id INT NOT NULL,
  PRIMARY KEY(id))
SQL
    execute "CREATE INDEX index_fkc_childtbl_on_parent_id ON fkc_childtbl (parent_id)"
    execute "ALTER TABLE fkc_childtbl ADD #{connection.foreign_key_constraint "parent_child_fkc", "parent_id", "fkc_parenttbl", "id"}"
  end

  def fkc_childtbl_def
    { "name"    => "fkc_childtbl",
      "columns" => [
        {"name" => "id",        "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "parent_id", "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
      ],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [
        {"name" => "index_fkc_childtbl_on_parent_id", "columns" => [1]}
      ] }
  end

  def create_adapterspecifictbl
    connection.create_adapterspecifictbl
  end

  def adapterspecifictbl_def(**args)
    connection.adapterspecifictbl_def(**args)
  end

  def adapterspecifictbl_row
    connection.adapterspecifictbl_row
  end

  def create_spatialtbl(srid: nil)
    execute(<<-SQL)
      CREATE TABLE spatialtbl (
        id INT NOT NULL,
        plainspat #{connection.spatial_column_type srid: srid} NOT NULL,
        pointspat #{connection.spatial_column_type srid: srid, geometry_type: 'point'},
        PRIMARY KEY(id))
SQL
    connection.create_spatial_index "plainidx", "spatialtbl", "plainspat" if connection.supports_spatial_indexes?
  end

  def remove_spatialtbl
    execute "DROP TABLE IF EXISTS spatialtbl"
  end

  def spatialtbl_def(srid: nil)
    { "name"    => "spatialtbl",
      "columns" => add_srid_to([
        {"name" => "id",        "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "plainspat", "column_type" => ColumnType::SPATIAL,    "nullable" => false},
        {"name" => "pointspat", "column_type" => ColumnType::SPATIAL, "subtype" => "point"},
      ].compact, srid),
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [
        ({"name" => "plainidx", "key_type" => "spatial", "columns" => [1]} if connection.supports_spatial_indexes?)
      ].compact }
  end

  def add_srid_to(columns, srid)
    return columns unless srid
    columns.each do |column|
      if column["column_type"] == ColumnType::SPATIAL
        column["column_type"] = ColumnType::SPATIAL_GEOGRAPHY
        column["reference_system"] = srid.to_s
      end
    end
  end

  def create_unsupportedtbl
    execute(<<-SQL)
      CREATE TABLE unsupportedtbl (
        pri INT NOT NULL,
        unsupported #{connection.unsupported_sql_type},
        PRIMARY KEY(pri))
SQL
  end

  def unsupportedtbl_def
    { "name"    => "unsupportedtbl",
      "columns" => [
        {"name" => "pri",         "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
        {"name" => "unsupported", "column_type" => connection.unsupported_column_type_name, "subtype" => connection.unsupported_sql_type}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_view
    execute(<<-SQL)
      CREATE VIEW ignoredview AS (SELECT col1, col3 FROM footbl)
SQL
  end
end
