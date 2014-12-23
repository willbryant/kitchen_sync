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
      "primary_key_columns" => [2, 1], # note order is that listed in the key, not the index of the column in the table
      "keys" => [
        {"name" => "secidx", "unique" => false, "columns" => [3]}] }
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
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_texttbl
    execute(<<-SQL)
      CREATE TABLE texttbl (
        pri INT NOT NULL,
        textfield TEXT#{'(268435456)' if @database_server == 'mysql'},
        PRIMARY KEY(pri))
SQL
  end

  def texttbl_def
    { "name"    => "texttbl",
      "columns" => [
        {"name" => "pri",       "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false},
        {"name" => "textfield", "column_type" => ColumnTypes::TEXT}],
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_misctbl
    execute(<<-SQL)
      CREATE TABLE misctbl (
        pri INT NOT NULL,
        boolfield BOOL,
        datefield DATE,
        timefield TIME,
        datetimefield #{@database_server == 'postgresql' ? 'timestamp' : 'DATETIME'},
        floatfield #{@database_server == 'postgresql' ? 'real' : 'FLOAT'},
        doublefield DOUBLE PRECISION,
        decimalfield DECIMAL(10, 4),
        vchrfield VARCHAR(9),
        fchrfield CHAR(9),
        textfield TEXT#{'(268435456)' if @database_server == 'mysql'},
        blobfield #{@database_server == 'postgresql' ? 'bytea' : 'BLOB'}#{'(268435456)' if @database_server == 'mysql'},
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
        {"name" => "textfield",     "column_type" => ColumnTypes::TEXT},
        {"name" => "blobfield",     "column_type" => ColumnTypes::BLOB}],
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_noprimarytbl(create_suitable_keys = true)
    execute(<<-SQL)
      CREATE TABLE noprimarytbl (
        nullable INT,
        version VARCHAR(255) NOT NULL,
        name VARCHAR(255),
        non_nullable INT NOT NULL)
SQL
    execute "CREATE INDEX everything_key ON noprimarytbl (name, nullable, version)"
    execute "CREATE UNIQUE INDEX ignored_key ON noprimarytbl (nullable, version)"
    execute "CREATE UNIQUE INDEX correct_key ON noprimarytbl (version)" if create_suitable_keys
    execute "CREATE UNIQUE INDEX version_and_name_key ON noprimarytbl (version, name)"
    execute "CREATE UNIQUE INDEX non_nullable_key ON noprimarytbl (non_nullable)" if create_suitable_keys
    execute "CREATE INDEX not_unique_key ON noprimarytbl (non_nullable)"
  end

  def noprimarytbl_def
    { "name" => "noprimarytbl",
      "columns" => [
        {"name" => "nullable",     "column_type" => ColumnTypes::SINT, "size" =>   4},
        {"name" => "version",      "column_type" => ColumnTypes::VCHR, "size" => 255, "nullable" => false},
        {"name" => "name",         "column_type" => ColumnTypes::VCHR, "size" => 255},
        {"name" => "non_nullable", "column_type" => ColumnTypes::SINT, "size" =>   4, "nullable" => false}],
      "primary_key_columns" => [1],
      "keys" => [ # sorted in uniqueness then alphabetic name order, but otherwise a transcription of the above create index statements
        {"name" => "correct_key",          "unique" => true,  "columns" => [1]},
        {"name" => "ignored_key",          "unique" => true,  "columns" => [0, 1]},
        {"name" => "non_nullable_key",     "unique" => true,  "columns" => [3]},
        {"name" => "version_and_name_key", "unique" => true,  "columns" => [1, 2]},
        {"name" => "everything_key",       "unique" => false, "columns" => [2, 0, 1]},
        {"name" => "not_unique_key",       "unique" => false, "columns" => [3]} ] }
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
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_defaultstbl
    execute(<<-SQL)
      CREATE TABLE defaultstbl (
        pri INT NOT NULL,
        varcharfield VARCHAR(32) DEFAULT 'test \\\\ with '' escaping©',
        charfield CHAR(5) DEFAULT 'test ',
        falseboolfield BOOL DEFAULT FALSE,
        trueboolfield BOOL DEFAULT TRUE,
        datefield DATE DEFAULT '2019-04-01',
        timefield TIME DEFAULT '12:34:56',
        datetimefield #{@database_server == 'postgresql' ? 'timestamp' : 'DATETIME'} DEFAULT '2019-04-01 12:34:56',
        floatfield #{@database_server == 'postgresql' ? 'real' : 'FLOAT'} DEFAULT 42.625,
        doublefield DOUBLE PRECISION DEFAULT 0.0625,
        decimalfield DECIMAL(9, 3) DEFAULT '123456.789',
        PRIMARY KEY(pri))
SQL
  end

  def defaultstbl_def
    { "name"    => "defaultstbl",
      "columns" => [
        {"name" => "pri",            "column_type" => ColumnTypes::SINT, "size" =>  4, "nullable" => false},
        {"name" => "varcharfield",   "column_type" => ColumnTypes::VCHR, "size" => 32,               "default_value" => "test \\ with ' escaping©"},
        {"name" => "charfield",      "column_type" => ColumnTypes::FCHR, "size" =>  5,               "default_value" => "test "},
        {"name" => "falseboolfield", "column_type" => ColumnTypes::BOOL,                             "default_value" => "false"},
        {"name" => "trueboolfield",  "column_type" => ColumnTypes::BOOL,                             "default_value" => "true"},
        {"name" => "datefield",      "column_type" => ColumnTypes::DATE,                             "default_value" => "2019-04-01"},
        {"name" => "timefield",      "column_type" => ColumnTypes::TIME,                             "default_value" => "12:34:56"},
        {"name" => "datetimefield",  "column_type" => ColumnTypes::DTTM,                             "default_value" => "2019-04-01 12:34:56"},
        {"name" => "floatfield",     "column_type" => ColumnTypes::REAL, "size" =>  4,               "default_value" => "42.625"},
        {"name" => "doublefield",    "column_type" => ColumnTypes::REAL, "size" =>  8,               "default_value" => "0.0625"},
        {"name" => "decimalfield",   "column_type" => ColumnTypes::DECI, "size" =>  9, "scale" => 3, "default_value" => "123456.789"}],
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def create_autotbl
    sequence_column_type = case @database_server
    when 'mysql'      then 'INT NOT NULL AUTO_INCREMENT'
    when 'postgresql' then 'SERIAL'
    end
    execute(<<-SQL)
      CREATE TABLE autotbl (
        inc #{sequence_column_type},
        payload INT NOT NULL,
        PRIMARY KEY(inc))
SQL
  end

  def autotbl_def
    { "name"    => "autotbl",
      "columns" => [
        {"name" => "inc",     "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false, "sequence" => ""},
        {"name" => "payload", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false}],
      "primary_key_columns" => [0],
      "keys" => [] }
  end
end
