require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def insert_secondtbl_rows
    execute "INSERT INTO secondtbl (tri, pri1, pri2, sec) VALUES (2, 2349174, 'xy', 1), (9, 968116383, 'aa', NULL)"
  end

  def assert_secondtbl_rows_present
    assert_equal [[9, 968116383, 'aa', nil],
                  [2,   2349174, 'xy',   1]],
                 query("SELECT * FROM secondtbl ORDER BY pri2, pri1")
  end

  def insert_footbl_rows
    execute "INSERT INTO footbl (col1, another_col, col3) VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str'), (1001, 0, 'last')"
  end

  def assert_footbl_rows_present
    assert_equal [[2,     10,       "test"],
                  [4,    nil,        "foo"],
                  [5,    nil,          nil],
                  [8,     -1, "longer str"],
                  [1001,   0,       "last"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  def insert_noprimarytbl_rows
    execute "INSERT INTO noprimarytbl (nullable, version, name, non_nullable) VALUES (2, 'a2349174', 'xy', 1), (NULL, 'b968116383', 'aa', 9)"
  end

  def assert_noprimarytbl_rows_present
    assert_equal [[2,     "a2349174", 'xy', 1],
                  [nil, "b968116383", 'aa', 9]],
                 query("SELECT * FROM noprimarytbl ORDER BY version") # sort order to match correct_key, which is the substitute primary key that get chosen
  end

  def assert_same_keys(table_def)
    table_name = table_def["name"]
    primary_key_columns = connection.table_key_columns(table_name)[connection.table_primary_key_name(table_name)]
    if table_def["primary_key_type"] == PrimaryKeyType::EXPLICIT_PRIMARY_KEY
      assert_equal table_def["primary_key_columns"].collect {|index| table_def["columns"][index]["name"]}, primary_key_columns
    else
      assert_nil primary_key_columns
    end

    assert_equal table_def["keys"].collect {|key| key["name"]}.sort, connection.table_keys(table_name).sort
    table_def["keys"].each {|key| assert_equal key["unique"], connection.table_keys_unique(table_name)[key["name"]], "#{key["name"]} index should#{' not' unless key["unique"]} be unique"}
    table_def["keys"].each {|key| assert_equal key["columns"].collect {|index| table_def["columns"][index]["name"]}, connection.table_key_columns(table_name)[key["name"]]}
  end

  test_each "accepts an empty list of tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => []]
    expect_sync_start_commands
    expect_quit_and_close
  end

  test_each "accepts a matching list of tables with matching schema" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["footbl"]
    send_command   Commands::RANGE, ["footbl", [], []]
    expect_command Commands::RANGE, ["middletbl"]
    send_command   Commands::RANGE, ["middletbl", [], []]
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    expect_quit_and_close
  end


  test_each "adds missing tables before other tables" do
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "adds missing tables between other tables" do
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "adds missing tables after other tables" do
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "adds all missing tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "drops extra tables before other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [middletbl_def, secondtbl_def]]
    read_command
    assert_equal %w(middletbl secondtbl), connection.tables
  end

  test_each "drops extra tables between other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def, secondtbl_def]]
    read_command
    assert_equal %w(footbl secondtbl), connection.tables
  end

  test_each "drops extra tables after other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def]]
    read_command
    assert_equal %w(footbl middletbl), connection.tables
  end

  test_each "drops all tables to match an empty list of tables on a non-empty database" do
    clear_schema
    create_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => []]
    read_command
    assert_equal %w(), connection.tables
  end


  test_each "can create all supported types of columns" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [misctbl_def]]
    read_command
    assert_equal %w(misctbl), connection.tables
  end


  test_each "doesn't complain about a missing table before other tables if told to ignore the table, and doesn't ask for its data" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'footbl'
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["middletbl"]
    send_command   Commands::RANGE, ["middletbl", [], []]
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "doesn't complain about a missing table between other tables if told to ignore the table" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'middletbl'
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["footbl"]
    send_command   Commands::RANGE, ["footbl", [], []]
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "doesn't complain about a missing table after other tables if told to ignore the table" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def, secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["footbl"]
    send_command   Commands::RANGE, ["footbl", [], []]
    expect_command Commands::RANGE, ["middletbl"]
    send_command   Commands::RANGE, ["middletbl", [], []]
    read_command
  end

  test_each "doesn't complain about extra tables before other tables if told to ignore the table" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'footbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [middletbl_def, secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["middletbl"]
    send_command   Commands::RANGE, ["middletbl", [], []]
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "doesn't complain about extra tables between other tables if told to ignore the table" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'middletbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def, secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["footbl"]
    send_command   Commands::RANGE, ["footbl", [], []]
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "doesn't complain about extra tables after other tables if told to ignore the table" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def, middletbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["footbl"]
    send_command   Commands::RANGE, ["footbl", [], []]
    expect_command Commands::RANGE, ["middletbl"]
    send_command   Commands::RANGE, ["middletbl", [], []]
    read_command
  end


  test_each "selects a substitute primary key if there's a table that has no primary key but that has unique key with only non-nullable columns" do
    clear_schema
    create_noprimarytbl(create_suitable_keys: true)
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [noprimarytbl_def(create_suitable_keys: true), secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["noprimarytbl"]
    send_command   Commands::RANGE, ["noprimarytbl", [], []]
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "complains if there's a table that has no unique key with only non-nullable columns" do
    clear_schema
    create_noprimarytbl(create_suitable_keys: false)
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA

    expect_stderr("Couldn't find a primary or non-nullable unique key on table noprimarytbl") do
      send_command Commands::SCHEMA, ["tables" => [noprimarytbl_def(create_suitable_keys: false), secondtbl_def]]
      read_command rescue nil
    end
  end

  test_each "doesn't complain if there's an ignored table that has no unique key with only non-nullable columns" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'noprimarytbl'
    clear_schema
    create_noprimarytbl(create_suitable_keys: false)
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [noprimarytbl_def(create_suitable_keys: false), secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "doesn't complain if there's an ignored table that has on unsupported column type" do
    program_env['ENDPOINT_IGNORE_TABLES'] = 'unsupportedtbl'
    clear_schema
    create_unsupportedtbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
  end

  test_each "complains if there's a non-ignored table that has on unsupported column type at both ends" do
    clear_schema
    create_unsupportedtbl
    create_secondtbl

    expect_handshake_commands
    expect_stderr("Don't know how to interpret type of unsupportedtbl.unsupported (#{connection.unsupported_column_type}).  Please check https://github.com/willbryant/kitchen_sync/blob/master/SCHEMA.md.") do
      expect_command Commands::SCHEMA
      send_command   Commands::SCHEMA, ["tables" => [unsupportedtbl_def, secondtbl_def]]
      read_command rescue nil
    end
  end

  test_each "complains if there's a non-ignored table that has on unsupported column type at the from end" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_stderr("Don't know how to interpret type of unsupportedtbl.unsupported (#{connection.unsupported_column_type}).  Please check https://github.com/willbryant/kitchen_sync/blob/master/SCHEMA.md.") do
      expect_command Commands::SCHEMA
      send_command   Commands::SCHEMA, ["tables" => [unsupportedtbl_def, secondtbl_def]]
      read_command rescue nil
    end
  end

  test_each "drops non-ignored tables that have on unsupported column type that exist only at the to end" do
    clear_schema
    create_unsupportedtbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [secondtbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["secondtbl"]
    send_command   Commands::RANGE, ["secondtbl", [], []]
    read_command
    assert_equal %w(secondtbl), connection.tables
  end


  test_each "adds missing columns before other columns" do
    clear_schema
    create_secondtbl
    execute("ALTER TABLE secondtbl DROP COLUMN tri")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def]]
    read_command
    assert_equal secondtbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("secondtbl")
    assert_match /^bigint/, connection.table_column_types("secondtbl")["tri"]
    assert_equal true, connection.table_column_nullability("secondtbl")["tri"]
    assert_equal nil, connection.table_column_defaults("secondtbl")["tri"]
  end

  test_each "adds missing columns between other columns without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl DROP COLUMN another_col")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_match /^smallint/, connection.table_column_types("footbl")["another_col"]
    assert_equal true, connection.table_column_nullability("footbl")["another_col"]
    assert_equal nil, connection.table_column_defaults("footbl")["another_col"]
    assert_equal [[2,    nil, nil], # the later columns get dropped and recreated too, because there's no other way to get the columns into the right order with postgresql
                  [4,    nil, nil],
                  [5,    nil, nil],
                  [8,    nil, nil],
                  [1001, nil, nil]],
                 query("SELECT * FROM footbl ORDER BY col1")
    assert_same_keys(footbl_def)
  end

  test_each "adds missing columns after other columns without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl DROP COLUMN col3")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_match /char.*10/, connection.table_column_types("footbl")["col3"]
    assert_equal true, connection.table_column_nullability("footbl")["col3"]
    assert_equal nil, connection.table_column_defaults("footbl")["col3"]
    assert_equal [[2,     10, nil],
                  [4,    nil, nil],
                  [5,    nil, nil],
                  [8,     -1, nil],
                  [1001,   0, nil]],
                 query("SELECT * FROM footbl ORDER BY col1")
    assert_same_keys(footbl_def)
  end

  test_each "adds missing non-nullable columns after other columns without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl DROP COLUMN another_col, DROP COLUMN col3")
    table_def = footbl_def
    table_def["columns"][1]["nullable"] = false
    table_def["columns"][2]["nullable"] = false

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal table_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_match /^smallint/, connection.table_column_types("footbl")["another_col"]
    assert_equal false, connection.table_column_nullability("footbl")["another_col"]
    assert_equal nil, connection.table_column_defaults("footbl")["another_col"]
    assert_match /char.*10/, connection.table_column_types("footbl")["col3"]
    assert_equal false, connection.table_column_nullability("footbl")["col3"]
    assert_equal nil, connection.table_column_defaults("footbl")["col3"]
    assert_equal [[2,    0, ""],
                  [4,    0, ""],
                  [5,    0, ""],
                  [8,    0, ""],
                  [1001, 0, ""]],
                 query("SELECT * FROM footbl ORDER BY col1")
    assert_same_keys(table_def)
  end

  test_each "recreates the table as necessary to add non-nullable columns with unique keys" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl DROP COLUMN col3")
    table_def = footbl_def
    table_def["columns"][2]["nullable"] = false
    table_def["keys"] << {"name" => "uniqueidx", "columns" => [2], "unique" => true}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal table_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_match /char.*10/, connection.table_column_types("footbl")["col3"]
    assert_equal false, connection.table_column_nullability("footbl")["col3"]
    assert_equal nil, connection.table_column_defaults("footbl")["col3"]
    assert_same_keys(table_def)
  end


  test_each "drops extra columns before other columns without recreating the table" do
    clear_schema
    execute(<<-SQL)
      CREATE TABLE footbl (
        removeme1 INT,
        removeme2 INT,
        col1 INT NOT NULL,
        another_col SMALLINT,
        col3 VARCHAR(10),
        PRIMARY KEY(col1))
SQL
    insert_footbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_footbl_rows_present
    assert_same_keys(footbl_def)
  end

  test_each "drops extra columns between other columns without recreating the table" do
    clear_schema
    execute(<<-SQL)
      CREATE TABLE footbl (
        col1 INT NOT NULL,
        removeme1 INT,
        removeme2 INT,
        another_col SMALLINT,
        col3 VARCHAR(10),
        PRIMARY KEY(col1))
SQL
    insert_footbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_footbl_rows_present
    assert_same_keys(footbl_def)
  end

  test_each "drops extra columns after other columns without recreating the table" do
    clear_schema
    execute(<<-SQL)
      CREATE TABLE footbl (
        col1 INT NOT NULL,
        another_col SMALLINT,
        col3 VARCHAR(10),
        removeme1 INT,
        removeme2 INT,
        PRIMARY KEY(col1))
SQL
    insert_footbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_footbl_rows_present
    assert_same_keys(footbl_def)
  end


  test_each "updates or recreates keys that included extra columns when they are removed, without recreating the table" do
    clear_schema
    execute(<<-SQL)
      CREATE TABLE secondtbl (
        tri BIGINT,
        pri1 INT NOT NULL,
        pri2 CHAR(2) NOT NULL,
        sec INT,
        removeme INT,
        PRIMARY KEY(pri2, pri1))
SQL
    execute(<<-SQL)
      CREATE INDEX removeidx ON secondtbl (removeme)
SQL
    execute(<<-SQL)
      CREATE INDEX secidx ON secondtbl (sec, removeme)
SQL
    insert_secondtbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def]]
    read_command
    assert_equal secondtbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("secondtbl")
    assert_secondtbl_rows_present
    assert_same_keys(secondtbl_def)
  end

  test_each "recreates tables if needed to update the primary key for dropped columns" do
    clear_schema
    execute(<<-SQL)
      CREATE TABLE footbl (
        col1 INT NOT NULL,
        another_col SMALLINT,
        col3 VARCHAR(10),
        removeme INT NOT NULL,
        PRIMARY KEY(col1, removeme))
SQL

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_same_keys(footbl_def)
  end

  test_each "moves misordered columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def.merge("columns" => footbl_def["columns"][1..-1] + footbl_def["columns"][0..0])]]
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    read_command
  end

  test_each "updates or recreates keys that consisted solely of columns that it moves" do
    # this scenario can create https://bugs.mysql.com/bug.php?id=57497
    clear_schema
    execute(<<-SQL)
      CREATE TABLE secondtbl (
        tri BIGINT,
        sec INT,
        pri1 INT NOT NULL,
        pri2 CHAR(2) NOT NULL,
        PRIMARY KEY(pri2, pri1))
SQL
    execute(<<-SQL)
      CREATE INDEX secidx ON secondtbl (sec)
SQL
    insert_secondtbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def]]
    read_command
    assert_equal secondtbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("secondtbl")
    assert_equal [[9, 968116383, 'aa', nil],
                  [2,   2349174, 'xy', nil]], # sec value has been lost
                 query("SELECT * FROM secondtbl ORDER BY pri2, pri1")
    assert_same_keys(secondtbl_def)
  end

  test_each "updates or recreates keys that included columns that it moves" do
    # this scenario can create https://bugs.mysql.com/bug.php?id=57497
    clear_schema
    execute(<<-SQL)
      CREATE TABLE secondtbl (
        tri BIGINT,
        sec INT,
        pri1 INT NOT NULL,
        pri2 CHAR(2) NOT NULL,
        PRIMARY KEY(pri2, pri1))
SQL
    execute(<<-SQL)
      CREATE INDEX secidx ON secondtbl (sec, tri)
SQL
    insert_secondtbl_rows
    added_def = secondtbl_def.merge("keys" => [secondtbl_def["keys"].first.merge("columns" => [3, 0])]) # note these indexes are for columns in the secondtbl_def order, not the above order

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [added_def]]
    read_command
    assert_equal added_def["columns"].collect {|column| column["name"]}, connection.table_column_names("secondtbl")
    assert_equal [[9, 968116383, 'aa', nil],
                  [2,   2349174, 'xy', nil]], # sec value has been lost
                 query("SELECT * FROM secondtbl ORDER BY pri2, pri1")
    assert_same_keys(added_def)
  end


  test_each "recreates the table if column types don't match" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col VARCHAR(11)",
        "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col TYPE VARCHAR(11)"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_match /^smallint/, connection.table_column_types("footbl")["another_col"]
  end


  test_each "makes columns nullable if they need to be, without recreating the table" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col SMALLINT NOT NULL",
        "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col SET NOT NULL"}[@database_server])
    execute "INSERT INTO footbl (col1, another_col, col3) VALUES (2, 10, 'test'), (4, 404, 'foo'), (5, 404, NULL), (8, -1, 'longer str'), (1001, 0, 'last')"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal({"col1" => false, "another_col" => true, "col3" => true}, connection.table_column_nullability("footbl"))
    assert_equal [[2,     10,       "test"],
                  [4,    404,        "foo"],
                  [5,    404,          nil],
                  [8,     -1, "longer str"],
                  [1001,   0,       "last"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "makes columns not nullable if they need to be, without recreating the table" do
    clear_schema
    create_footbl
    table_def = footbl_def
    table_def["columns"][1]["nullable"] = false
    execute "INSERT INTO footbl (col1, another_col, col3) VALUES (2, 10, 'test'), (4, 404, 'foo'), (5, 404, NULL), (8, -1, 'longer str'), (1001, 0, 'last')"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal({"col1" => false, "another_col" => false, "col3" => true}, connection.table_column_nullability("footbl"))
    assert_equal [[2,     10,       "test"],
                  [4,    404,        "foo"],
                  [5,    404,          nil],
                  [8,     -1, "longer str"],
                  [1001,   0,       "last"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "makes columns not nullable if they need to be, without recreating the table, even if there are some null values to start with, keeping existing values where already not null" do
    clear_schema
    create_footbl
    table_def = footbl_def
    table_def["columns"][1]["nullable"] = false
    execute "INSERT INTO footbl (col1, another_col, col3) VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str'), (1001, 0, 'last')"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal({"col1" => false, "another_col" => false, "col3" => true}, connection.table_column_nullability("footbl"))
    assert_equal [[2,     10,       "test"],
                  [4,      0,        "foo"],
                  [5,      0,          nil],
                  [8,     -1, "longer str"],
                  [1001,   0,       "last"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "makes columns not nullable if they need to be, without recreating the table, even if there are some null values to start with, for any type of column" do
    clear_schema
    create_misctbl
    table_def = misctbl_def
    table_def["columns"].each {|column| column["nullable"] = false}
    execute "INSERT INTO misctbl (pri) VALUES (42)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal([false], connection.table_column_nullability("misctbl").values.uniq)
    rows = query("SELECT * FROM misctbl")
    assert_equal 1, rows.size
    assert_equal 42, rows[0][0]
    assert_equal false, rows[0][1]
    assert_equal Date.new(2000, 1, 1), rows[0][2]
    assert_equal connection.zero_time_value, rows[0][3] # not consistent between ruby clients
    assert_equal Time.new(2000, 1, 1, 0, 0, 0), rows[0][4]
    assert_equal 0, rows[0][5]
    assert_equal 0, rows[0][6]
    assert_equal 0, BigDecimal(rows[0][7].to_s) # return type not consistent between ruby clients
    assert_equal "", rows[0][8]
    assert_equal "", rows[0][9].strip # padding not consistent between ruby clients
    assert_equal "00000000-0000-0000-0000-000000000000", rows[0][10]
    assert_equal "", rows[0][11]
    assert_equal "", rows[0][12]
  end

  test_each "recreates the table as necessary to make columns not nullable if they have unique keys" do
    clear_schema
    create_footbl
    insert_footbl_rows
    table_def = footbl_def
    table_def["columns"][1]["nullable"] = false
    table_def["keys"] << {"name" => "uniqueidx", "columns" => [1], "unique" => true}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal table_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_equal false, connection.table_column_nullability("footbl")["another_col"]
    assert_same_keys(table_def)
  end


  test_each "changes the column default if they need to be cleared, without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl ALTER another_col SET DEFAULT 42")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal({"col1" => nil, "another_col" => nil, "col3" => nil}, connection.table_column_defaults("footbl"))
    assert_footbl_rows_present
  end

  test_each "changes the column default if they need to be cleared on non-nullable columns, without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl ALTER col1 SET DEFAULT 42")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_equal({"col1" => nil, "another_col" => nil, "col3" => nil}, connection.table_column_defaults("footbl"))
    assert_footbl_rows_present
  end

  test_each "changes the column default if they need to be set, without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    table_def = footbl_def
    table_def["columns"][1]["default_value"] = "42"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal({"col1" => nil, "another_col" => "42", "col3" => nil}, connection.table_column_defaults("footbl"))
    assert_footbl_rows_present
  end

  test_each "changes the column default if they need to be changed, without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    execute("ALTER TABLE footbl ALTER another_col SET DEFAULT 42")
    table_def = footbl_def
    table_def["columns"][1]["default_value"] = "23"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal({"col1" => nil, "another_col" => "23", "col3" => nil}, connection.table_column_defaults("footbl"))
    assert_footbl_rows_present
  end

  test_each "changes the column default if they need to be set on a string column, without recreating the table" do
    clear_schema
    create_footbl
    insert_footbl_rows
    table_def = footbl_def
    table_def["columns"][2]["default_value"] = "foo"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [table_def]]
    read_command
    assert_equal({"col1" => nil, "another_col" => nil, "col3" => "foo"}, connection.table_column_defaults("footbl"))
    assert_footbl_rows_present
  end


  test_each "creates missing tables with sequence primary key columns" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [autotbl_def]]
    read_command
    assert_equal({"inc" => true, "payload" => false}, connection.table_column_sequences("autotbl"))
  end


  test_each "recreates the table if the primary key column order doesn't match" do
    clear_schema
    create_secondtbl
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def.merge("primary_key_columns" => [1, 2])]]
    read_command
    assert_equal [1, 2].collect {|index| secondtbl_def["columns"][index]["name"]}, connection.table_key_columns("secondtbl")[connection.table_primary_key_name("secondtbl")]
    assert_equal [], query("SELECT * FROM secondtbl")
  end

  test_each "recreates the table if there are extra primary key columns after the matching part" do
    clear_schema
    create_secondtbl
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def.merge("primary_key_columns" => [2, 1, 3])]]
    read_command
    assert_equal [2, 1, 3].collect {|index| secondtbl_def["columns"][index]["name"]}, connection.table_key_columns("secondtbl")[connection.table_primary_key_name("secondtbl")]
    assert_equal [], query("SELECT * FROM secondtbl")
  end

  test_each "recreates the table if there are extra primary key columns before the matching part" do
    clear_schema
    create_secondtbl
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def.merge("primary_key_columns" => [3, 2, 1])]]
    read_command
    assert_equal [3, 2, 1].collect {|index| secondtbl_def["columns"][index]["name"]}, connection.table_key_columns("secondtbl")[connection.table_primary_key_name("secondtbl")]
    assert_equal [], query("SELECT * FROM secondtbl")
  end


  test_each "drops extra keys, without recreating the table" do
    clear_schema
    create_secondtbl
    insert_secondtbl_rows
    execute "CREATE INDEX extrakey ON secondtbl (sec, tri)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def]]
    read_command
    assert_equal secondtbl_def["keys"].collect {|key| key["name"]}, connection.table_keys("secondtbl")
    assert_secondtbl_rows_present
  end

  test_each "adds missing keys, without recreating the table" do
    clear_schema
    create_secondtbl
    insert_secondtbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def.merge("keys" => secondtbl_def["keys"] + [secondtbl_def["keys"].first.merge("name" => "missingkey")])]]
    read_command
    assert_equal %w(missingkey) + secondtbl_def["keys"].collect {|key| key["name"]}, connection.table_keys("secondtbl").sort
    assert !connection.table_keys_unique("secondtbl")["missingkey"], "missingkey index should not be unique"
    assert_secondtbl_rows_present
  end

  test_each "changes keys whose unique flag doesn't match, without recreating the table" do
    clear_schema
    create_secondtbl
    insert_secondtbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    key = secondtbl_def["keys"].first
    assert !connection.table_keys_unique("secondtbl")[key["name"]], "missingkey index should not be unique before test"
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def.merge("keys" => [key.merge("unique" => true)])]]
    read_command
    assert connection.table_keys_unique("secondtbl")[key["name"]], "missingkey index should be unique"
    assert_secondtbl_rows_present
  end

  test_each "changes keys whose column list doesn't match, without recreating the table" do
    clear_schema
    create_secondtbl
    insert_secondtbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    key = secondtbl_def["keys"].first
    assert_not_equal [secondtbl_def["columns"][3]["name"], secondtbl_def["columns"][1]["name"]], connection.table_key_columns("secondtbl")[key["name"]]
    send_command Commands::SCHEMA, ["tables" => [secondtbl_def.merge("keys" => [key.merge("columns" => [3, 1])])]]
    read_command
    assert_equal [secondtbl_def["columns"][3]["name"], secondtbl_def["columns"][1]["name"]], connection.table_key_columns("secondtbl")[key["name"]]
    assert_secondtbl_rows_present
  end

  test_each "changes keys whose column list doesn't match on tables with no explicit primary key, without recreating the table" do
    clear_schema
    create_noprimarytbl
    insert_noprimarytbl_rows

    expect_handshake_commands
    expect_command Commands::SCHEMA
    key = noprimarytbl_def["keys"].last
    assert_not_equal [noprimarytbl_def["columns"][3]["name"], noprimarytbl_def["columns"][1]["name"]], connection.table_key_columns("noprimarytbl")[key["name"]]
    send_command Commands::SCHEMA, ["tables" => [noprimarytbl_def.merge("keys" => noprimarytbl_def["keys"][0..-2] + [key.merge("columns" => [3, 1])])]]
    read_command
    assert_equal [noprimarytbl_def["columns"][3]["name"], noprimarytbl_def["columns"][1]["name"]], connection.table_key_columns("noprimarytbl")[key["name"]]
    assert_noprimarytbl_rows_present
  end

  test_each "creates tables with explicit primary keys" do
    clear_schema
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [footbl_def]]
    read_command
    assert_same_keys(footbl_def)
  end

  test_each "creates tables without explicit primary keys" do
    clear_schema
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [noprimarytbl_def(create_suitable_keys: true)]]
    read_command
    assert_same_keys(noprimarytbl_def(create_suitable_keys: true))
  end

  test_each "skips schema definitions it doesn't recognise" do
    clear_schema
    expect_handshake_commands
    expect_command Commands::SCHEMA

    values_of_many_types = [-(2**33), -(2**17), -(2**9), -(2**4), *-16..16, 2**4, 2**9, 2**17, 2**33, 1.0, nil, true, false, "a", "a"*17, "a"*(2**17), ["foo", "bar"], {"foo" => "bar"}]
    table = {"before_table_stuff" => values_of_many_types}.merge(secondtbl_def).merge("after_table_stuff" => values_of_many_types)
    table["columns"][0] = {"before_column_stuff" => values_of_many_types}.merge(table["columns"][0]).merge("after_column_stuff" => values_of_many_types)
    table["keys"][0] = {"before_key_stuff" => values_of_many_types}.merge(table["keys"][0]).merge("after_key_stuff" => values_of_many_types)
    send_command Commands::SCHEMA, ["before_tables" => values_of_many_types, "tables" => [table], "after_tables" => values_of_many_types]
    read_command

    assert_equal secondtbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("secondtbl")
    assert_match /^bigint/, connection.table_column_types("secondtbl")["tri"]
    assert_equal true, connection.table_column_nullability("secondtbl")["tri"]
    assert_equal nil, connection.table_column_defaults("secondtbl")["tri"]
    assert_same_keys(secondtbl_def)
  end

  ENDPOINT_DATABASES.keys do |from_database|
    test_each "can run the table creation code for adapter-specific columns from #{from_database}" do
      clear_schema
      create_adapterspecifictbl(from_database)

      tbldef = adapterspecifictbl_def(from_database)
      expect_handshake_commands
      expect_command Commands::SCHEMA
      send_command Commands::SCHEMA, [{"tables" => [tbldef]}]
      read_command

      assert_equal tbldef["columns"].collect {|column| column["name"]}, connection.table_column_names(tbldef["name"])
    end
  end
end
