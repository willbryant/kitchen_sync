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
        {"name" => "col1",        "column_type" => ColumnTypes::SINT, "size" =>  4, "nullable" => false},
        {"name" => "another_col", "column_type" => ColumnTypes::SINT, "size" =>  2},
        {"name" => "col3",        "column_type" => ColumnTypes::VCHR, "size" => 10}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
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
        {"name" => "tri",  "column_type" => ColumnTypes::SINT, "size" => 8},
        {"name" => "pri1", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "pri2", "column_type" => ColumnTypes::FCHR, "size" => 2, "nullable" => false},
        {"name" => "sec",  "column_type" => ColumnTypes::SINT, "size" => 4}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [2, 1], # note order is that listed in the key, not the index of the column in the table
      "keys" => [
        {"name" => "secidx", "unique" => false, "columns" => [3]}] }
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
        {"name" => "pri", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "sec", "column_type" => ColumnTypes::SINT, "size" => 4},
        {"name" => "col3", "column_type" => ColumnTypes::VCHR, "size" => 1000}],
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
        {"name" => "pri", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false}],
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
        {"name" => "pri",       "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "textfield", "column_type" => ColumnTypes::TEXT}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_misctbl
    connection.create_enum_column_type
    execute(<<-SQL)
      CREATE TABLE misctbl (
        pri INT NOT NULL,
        boolfield BOOL,
        datefield DATE,
        timefield TIME,
        datetimefield #{connection.datetime_column_type},
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
        {"name" => "pri",           "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "boolfield",     "column_type" => ColumnTypes::BOOL},
        {"name" => "datefield",     "column_type" => ColumnTypes::DATE},
        {"name" => "timefield",     "column_type" => ColumnTypes::TIME},
        {"name" => "datetimefield", "column_type" => ColumnTypes::DTTM},
        {"name" => "floatfield",    "column_type" => ColumnTypes::REAL, "size" => 4},
        {"name" => "doublefield",   "column_type" => ColumnTypes::REAL, "size" => 8},
        {"name" => "decimalfield",  "column_type" => ColumnTypes::DECI, "size" => 10, "scale" => 4},
        {"name" => "vchrfield",     "column_type" => ColumnTypes::VCHR, "size" => 9},
        {"name" => "fchrfield",     "column_type" => ColumnTypes::FCHR, "size" => 9},
        {"name" => "uuidfield",     "column_type" => ColumnTypes::UUID},
        {"name" => "textfield",     "column_type" => ColumnTypes::TEXT},
        {"name" => "blobfield",     "column_type" => ColumnTypes::BLOB},
        {"name" => "jsonfield",     "column_type" => ColumnTypes::JSON},
        {"name" => "enumfield",     "column_type" => ColumnTypes::ENUM, "enumeration_values" => ["red", "green", "blue", "with'quote"]}.merge(connection.enum_column_type_restriction),
      ],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
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
        {"name" => "nullable",     "column_type" => ColumnTypes::SINT, "size" =>   4},
        {"name" => "version",      "column_type" => ColumnTypes::VCHR, "size" => 255, "nullable" => false},
        {"name" => "name",         "column_type" => ColumnTypes::VCHR, "size" => 255},
        {"name" => "non_nullable", "column_type" => ColumnTypes::SINT, "size" =>   4, "nullable" => false}],
      "primary_key_columns" => create_suitable_keys ? [1] : [3],
      "primary_key_type" => create_suitable_keys ? PrimaryKeyType::SUITABLE_UNIQUE_KEY : PrimaryKeyType::PARTIAL_KEY,
      "secondary_sort_columns" => create_suitable_keys ? nil : [0, 1, 2],
      "keys" => [ # sorted in uniqueness then alphabetic name order, but otherwise a transcription of the above create index statements
        ({"name" => "correct_key",          "unique" => true,  "columns" => [1]} if create_suitable_keys),
        {"name" => "ignored_key",          "unique" => true,  "columns" => [0, 1]},
        ({"name" => "non_nullable_key",     "unique" => true,  "columns" => [3]} if create_suitable_keys),
        {"name" => "version_and_name_key", "unique" => true,  "columns" => [1, 2]},
        {"name" => "everything_key",       "unique" => false, "columns" => [2, 0, 1, 3]},
        {"name" => "not_unique_key",       "unique" => false, "columns" => [3]} ].compact }.
      reject {|k, v| v.nil?}
  end

  def create_noprimaryjointbl(create_keys: true)
    execute(<<-SQL)
      CREATE TABLE noprimaryjointbl (
        table1_id INT NOT NULL,
        table2_id INT NOT NULL)
SQL
    execute "CREATE INDEX index_by_table1_id ON noprimaryjointbl (table1_id)" if create_keys
    execute "CREATE INDEX index_by_table2_id_and_table1_id ON noprimaryjointbl (table2_id, table1_id)" if create_keys
  end

  def noprimaryjointbl_def(create_keys: true)
    { "name" => "noprimaryjointbl",
      "columns" => [
        {"name" => "table1_id", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "table2_id", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false}],
      "primary_key_columns" => create_keys ? [1, 0] : [], # if index_by_table2_id exists, it will be chosen because it covers all the columns
      "primary_key_type" => create_keys ? PrimaryKeyType::PARTIAL_KEY : PrimaryKeyType::NO_AVAILABLE_KEY,
      "keys" => create_keys ? [
        {"name" => "index_by_table1_id",               "unique" => false, "columns" => [0]},
        {"name" => "index_by_table2_id_and_table1_id", "unique" => false, "columns" => [1, 0]}
      ] : [] }.
      reject {|k, v| v.nil?}
  end

  def create_some_tables
    clear_schema
    create_footbl
    create_secondtbl
  end

  def some_table_defs
    [footbl_def, secondtbl_def]
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
        timefield TIME DEFAULT '12:34:56',
        datetimefield #{connection.datetime_column_type} DEFAULT '2019-04-01 12:34:56',
        #{"currentdatetimefield #{connection.datetime_column_type} DEFAULT CURRENT_TIMESTAMP," if connection.supports_multiple_timestamp_columns?}
        floatfield #{connection.real_column_type} DEFAULT 42.625,
        doublefield DOUBLE PRECISION DEFAULT 0.0625,
        decimalfield DECIMAL(9, 3) DEFAULT '123456.789',
        PRIMARY KEY(pri))
SQL
  end

  def defaultstbl_def
    { "name"    => "defaultstbl",
      "columns" => [
        {"name" => "pri",                  "column_type" => ColumnTypes::SINT, "size" =>  4, "nullable" => false},
        {"name" => "varcharfield",         "column_type" => ColumnTypes::VCHR, "size" => 32,               "default_value" => "test \\ with ' escaping©"},
        {"name" => "emptydefaultstr",      "column_type" => ColumnTypes::VCHR, "size" => 255, "default_value" => ""},
        {"name" => "spacedefaultstr",      "column_type" => ColumnTypes::VCHR, "size" => 255, "default_value" => " "},
        {"name" => "zerodefaultstr",       "column_type" => ColumnTypes::VCHR, "size" => 255, "default_value" => "0"},
        {"name" => "nodefaultstr",         "column_type" => ColumnTypes::VCHR, "size" => 255},
        {"name" => "charfield",            "column_type" => ColumnTypes::FCHR, "size" =>  5,               "default_value" => "test "},
        {"name" => "falseboolfield",       "column_type" => ColumnTypes::BOOL,                             "default_value" => "false"},
        {"name" => "trueboolfield",        "column_type" => ColumnTypes::BOOL,                             "default_value" => "true"},
        {"name" => "datefield",            "column_type" => ColumnTypes::DATE,                             "default_value" => "2019-04-01"},
        {"name" => "timefield",            "column_type" => ColumnTypes::TIME,                             "default_value" => "12:34:56"},
        {"name" => "datetimefield",        "column_type" => ColumnTypes::DTTM,                             "default_value" => "2019-04-01 12:34:56"},
        ({"name" => "currentdatetimefield", "column_type" => ColumnTypes::DTTM,                             "default_function" => "CURRENT_TIMESTAMP"} if connection.supports_multiple_timestamp_columns?),
        {"name" => "floatfield",           "column_type" => ColumnTypes::REAL, "size" =>  4,               "default_value" => "42.625"},
        {"name" => "doublefield",          "column_type" => ColumnTypes::REAL, "size" =>  8,               "default_value" => "0.0625"},
        {"name" => "decimalfield",         "column_type" => ColumnTypes::DECI, "size" =>  9, "scale" => 3, "default_value" => "123456.789"}
      ].compact,
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_autotbl
    execute(<<-SQL)
      CREATE TABLE autotbl (
        inc #{connection.sequence_column_type},
        payload INT NOT NULL,
        PRIMARY KEY(inc))
SQL
  end

  def autotbl_def
    { "name"    => "autotbl",
      "columns" => [
        {"name" => "inc",     "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false, "sequence" => ""},
        {"name" => "payload", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false}],
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_adapterspecifictbl(database_server = @database_server)
    case database_server
    when 'mysql'
      execute(<<-SQL)
        CREATE TABLE ```mysql``tbl` (
          pri INT UNSIGNED NOT NULL AUTO_INCREMENT,
          tiny2 TINYINT(2) UNSIGNED DEFAULT 99,
          nulldefaultstr VARCHAR(255) DEFAULT NULL,
          timestampboth TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          #{"timestampcreateonly TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," if connection.supports_multiple_timestamp_columns?}
          #{"mysqlfunctiondefault VARCHAR(255) DEFAULT (uuid())," if connection.mysql_default_expressions?}
          `select` INT,
          ```quoted``` INT,
          PRIMARY KEY(pri))
SQL

    when 'postgresql'
      # note default_expressions? is always true for postgresql itself, but some columns here are conditional for the benefit of the
      # mysql 5.7 cross-compatibility tests; these still run for the mysql 8/mariadb cross-compatibility tests
      execute(<<-SQL)
        CREATE TABLE """postgresql""tbl" (
          pri #{connection.supports_generated_as_identity? ? 'integer GENERATED ALWAYS AS IDENTITY' : 'SERIAL'},
          uu UUID NOT NULL,
          nolengthvaryingfield CHARACTER VARYING,
          noprecisionnumericfield NUMERIC,
          nulldefaultstr VARCHAR(255) DEFAULT NULL,
          #{"currentdatefield DATE DEFAULT CURRENT_DATE," if connection.default_expressions?}
          #{"currentuserdefault VARCHAR(255) DEFAULT current_user," if connection.default_expressions?}
          #{"pgfunctiondefault TEXT DEFAULT version()," if connection.default_expressions?}
          timewithzone time with time zone,
          timestampwithzone timestamp with time zone,
          "select" INT,
          "\"\"quoted\"\"" INT,
          PRIMARY KEY(pri))
SQL
    end
  end

  def adapterspecifictbl_def(database_server = @database_server)
    case database_server
    when 'mysql'
      { "name"    => "`mysql`tbl",
        "columns" => [
          {"name" => "pri",                  "column_type" => ColumnTypes::UINT, "size" =>  4, "nullable" => false, "sequence" => ""},
          {"name" => "tiny2",                "column_type" => ColumnTypes::UINT, "size" =>  1, "default_value" => "99"}, # note we've lost the (nonportable) display width (2) - size tells us the size of the integers, not the display width
          {"name" => "nulldefaultstr",       "column_type" => ColumnTypes::VCHR, "size" => 255},
          {"name" => "timestampboth",        "column_type" => ColumnTypes::DTTM,               "nullable" => false, "default_function" => "CURRENT_TIMESTAMP", "mysql_timestamp" => true, "mysql_on_update_timestamp" => true},
          ({"name" => "timestampcreateonly", "column_type" => ColumnTypes::DTTM,               "nullable" => false, "default_function" => "CURRENT_TIMESTAMP", "mysql_timestamp" => true} if connection.supports_multiple_timestamp_columns?),
          ({"name" => "mysqlfunctiondefault", "column_type" => ColumnTypes::VCHR, "size" => 255,                     "default_function" => "uuid()"} if connection.mysql_default_expressions?),
          {"name" => "select",               "column_type" => ColumnTypes::SINT, "size" =>  4},
          {"name" => "`quoted`",             "column_type" => ColumnTypes::SINT, "size" =>  4},
        ].compact,
        "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
        "primary_key_columns" => [0],
        "keys" => [] }

    when 'postgresql'
      { "name"    => "\"postgresql\"tbl",
        "columns" => [
          {"name" => "pri",                  "column_type" => ColumnTypes::SINT, "size" =>  4,  "nullable" => false, "sequence" => ""}.merge(connection.supports_generated_as_identity? ? {"identity_generated_always" => true} : {}),
          {"name" => "uu",                   "column_type" => ColumnTypes::UUID,                "nullable" => false},
          {"name" => "nolengthvaryingfield", "column_type" => ColumnTypes::VCHR},
          {"name" => "noprecisionnumericfield", "column_type" => ColumnTypes::DECI},
          {"name" => "nulldefaultstr",       "column_type" => ColumnTypes::VCHR, "size" => 255,                      "default_function" => "NULL"}, # note different to mysql, where no default and DEFAULT NULL are the same thing
          ({"name" => "currentdatefield",     "column_type" => ColumnTypes::DATE,                                     "default_function" => CaseInsensitiveString.new("CURRENT_DATE")} if connection.default_expressions?), # only conditional for the benefit of the mysql 5.7 cross-compatibility tests
          ({"name" => "currentuserdefault",   "column_type" => ColumnTypes::VCHR, "size" => 255,                      "default_function" => CaseInsensitiveString.new("CURRENT_USER")} if connection.default_expressions?),
          ({"name" => "pgfunctiondefault",    "column_type" => ColumnTypes::TEXT,                                     "default_function" => "version()"} if connection.default_expressions?),
          {"name" => "timewithzone",         "column_type" => ColumnTypes::TIME, "time_zone" => true},
          {"name" => "timestampwithzone",    "column_type" => ColumnTypes::DTTM, "time_zone" => true},
          {"name" => "select",               "column_type" => ColumnTypes::SINT, "size" => 4},
          {"name" => "\"quoted\"",           "column_type" => ColumnTypes::SINT, "size" =>  4},
        ].compact,
        "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
        "primary_key_columns" => [0],
        "keys" => [] }
    end
  end

  def adapterspecifictbl_row(database_server = @database_server)
    case database_server
    when 'mysql'
      { "tiny2" => 12,
        "timestampboth" => "2019-07-03 00:00:01" }

    when 'postgresql'
      { "uu" => "3d190b75-dbb1-4d34-a41e-d590c1c8a895",
        "nolengthvaryingfield" => "test data",
        "noprecisionnumericfield" => "1234567890.0987654321" }
    end
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
        {"name" => "id",                   "column_type" => ColumnTypes::SINT, "size" =>  4, "nullable" => false},
        {"name" => "plainspat",            "column_type" => ColumnTypes::SPAT,               "nullable" => false},
        {"name" => "pointspat",            "column_type" => ColumnTypes::SPAT,                                   "type_restriction" => "point"},
      ].compact, srid),
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [
        ({"name" => "plainidx", "key_type" => "spatial", "columns" => [1]} if connection.supports_spatial_indexes?)
      ].compact }
  end

  def add_srid_to(columns, srid)
    return columns unless srid
    columns.each {|column| column["reference_system"] = srid.to_s if column["column_type"] == ColumnTypes::SPAT}
  end

  def create_unsupportedtbl
    execute(<<-SQL)
      CREATE TABLE unsupportedtbl (
        pri INT NOT NULL,
        unsupported #{connection.unsupported_column_type},
        PRIMARY KEY(pri))
SQL
  end

  def unsupportedtbl_def
    { "name"    => "unsupportedtbl",
      "columns" => [
        {"name" => "pri",         "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "unsupported", "column_type" => ColumnTypes::UNKN, "db_type_def" => connection.unsupported_column_type}],
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
