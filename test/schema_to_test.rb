require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def insert_secondtbl_rows
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1), (9, 968116383, 'aa', NULL)"
  end

  def assert_secondtbl_rows_present
    assert_equal [[9, 968116383, 'aa', nil],
                  [2,   2349174, 'xy',   1]],
                 query("SELECT * FROM secondtbl ORDER BY pri2, pri1")
  end

  test_each "accepts an empty list of tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => []
    expect_command Commands::QUIT
  end

  test_each "accepts a matching list of tables with matching schema" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::QUIT
  end


  test_each "adds missing tables before other tables" do
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "adds missing tables between other tables" do
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "adds missing tables after other tables" do
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    read_command
    assert_equal %w(footbl middletbl secondtbl), connection.tables
  end

  test_each "adds all missing tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
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
    send_command Commands::SCHEMA, "tables" => [middletbl_def, secondtbl_def]
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
    send_command Commands::SCHEMA, "tables" => [footbl_def, secondtbl_def]
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
    send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def]
    read_command
    assert_equal %w(footbl middletbl), connection.tables
  end

  test_each "drops all tables to match an empty list of tables on a non-empty database" do
    clear_schema
    create_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => []
    read_command
    assert_equal %w(), connection.tables
  end


  test_each "doesn't complain about a missing table before other tables if told to ignore the table, and doesn't ask for its data" do
    program_args << 'footbl'
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about a missing table between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about a missing table after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about extra tables before other tables if told to ignore the table" do
    program_args << 'footbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about extra tables between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end

  test_each "doesn't complain about extra tables after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    read_command
  end


  test_each "adds missing columns before other columns" do
    clear_schema
    create_secondtbl
    execute("ALTER TABLE secondtbl DROP COLUMN tri")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [secondtbl_def]
    read_command
    assert_equal secondtbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("secondtbl")
    assert_match /^bigint/, connection.table_column_types("secondtbl")["tri"]
    assert_equal true, connection.table_column_nullability("secondtbl")["tri"]
    assert_equal nil, connection.table_column_defaults("secondtbl")["tri"]
  end

  test_each "adds missing columns between other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN another_col")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_match /^smallint/, connection.table_column_types("footbl")["another_col"]
    assert_equal true, connection.table_column_nullability("footbl")["another_col"]
    assert_equal nil, connection.table_column_defaults("footbl")["another_col"]
  end

  test_each "adds missing columns after other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN col3")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def]
    read_command
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    assert_match /^smallint/, connection.table_column_types("footbl")["another_col"]
    assert_equal true, connection.table_column_nullability("footbl")["another_col"]
    assert_equal nil, connection.table_column_defaults("footbl")["another_col"]
  end

  test_each "drops extra columns before other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    columns = footbl_def["columns"][1..-1]
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => columns)]
    read_command
    assert_equal columns.collect {|column| column["name"]}, connection.table_column_names("footbl")
  end

  test_each "drops extra columns between other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    columns = footbl_def["columns"][0..0] + footbl_def["columns"][2..-1]
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => columns)]
    read_command
    assert_equal columns.collect {|column| column["name"]}, connection.table_column_names("footbl")
  end

  test_each "drops extra columns after other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    columns = footbl_def["columns"][0..-2]
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => columns)]
    read_command
    assert_equal columns.collect {|column| column["name"]}, connection.table_column_names("footbl")
  end

  test_each "moves misordered columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => footbl_def["columns"][1..-1] + footbl_def["columns"][0..0])]
    assert_equal footbl_def["columns"].collect {|column| column["name"]}, connection.table_column_names("footbl")
    read_command
  end


  test_each "recreates the table if column types don't match" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col VARCHAR(11)", "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col TYPE VARCHAR(11)"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def]
    read_command
    assert_match /^smallint/, connection.table_column_types("footbl")["another_col"]
  end


  test_each "recreates the table if columns need to be made nullable" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col SMALLINT NOT NULL",
        "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col SET NOT NULL"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def]
    read_command
    assert_equal({"col1" => false, "another_col" => true, "col3" => true}, connection.table_column_nullability("footbl"))
  end

  test_each "recreates the table if columns need to be made not nullable" do
    clear_schema
    create_footbl
    table_def = footbl_def
    table_def["columns"][1]["nullable"] = false

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [table_def]
    read_command
    assert_equal({"col1" => false, "another_col" => false, "col3" => true}, connection.table_column_nullability("footbl"))
  end


  test_each "recreates the table if column defaults need to be cleared" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl ALTER another_col SET DEFAULT 42")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [footbl_def]
    read_command
    assert_equal({"col1" => nil, "another_col" => nil, "col3" => nil}, connection.table_column_defaults("footbl"))
  end

  test_each "recreates the table if column defaults need to be set" do
    clear_schema
    create_footbl
    table_def = footbl_def
    table_def["columns"][1]["default_value"] = "42"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [table_def]
    read_command
    assert_equal({"col1" => nil, "another_col" => "42", "col3" => nil}, connection.table_column_defaults("footbl"))
  end

  test_each "recreates the table if column defaults need to be changed" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl ALTER another_col SET DEFAULT 42")
    table_def = footbl_def
    table_def["columns"][1]["default_value"] = "23"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [table_def]
    read_command
    assert_equal({"col1" => nil, "another_col" => "23", "col3" => nil}, connection.table_column_defaults("footbl"))
  end

  test_each "recreates the table if column defaults need to be set on a string column" do
    clear_schema
    create_footbl
    table_def = footbl_def
    table_def["columns"][2]["default_value"] = "foo"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [table_def]
    read_command
    assert_equal({"col1" => nil, "another_col" => nil, "col3" => "foo"}, connection.table_column_defaults("footbl"))
  end


  test_each "recreates the table if the primary key column order doesn't match" do
    clear_schema
    create_secondtbl
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [1, 2])]
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
    send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [2, 1, 3])]
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
    send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [3, 2, 1])]
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
    send_command Commands::SCHEMA, "tables" => [secondtbl_def]
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
    send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => secondtbl_def["keys"] + [secondtbl_def["keys"].first.merge("name" => "missingkey")])]
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
    send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => [key.merge("unique" => true)])]
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
    send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => [key.merge("columns" => [3, 1])])]
    read_command
    assert_equal [secondtbl_def["columns"][3]["name"], secondtbl_def["columns"][1]["name"]], connection.table_key_columns("secondtbl")[key["name"]]
    assert_secondtbl_rows_present
  end
end
