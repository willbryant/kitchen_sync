require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

require 'tempfile'

class FilterFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "sees an empty table for the RANGE command for tables with a clear attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    send_handshake_commands(filters: {"footbl" => {"where_conditions" => "false"}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA, [{"tables" => [footbl_def, secondtbl_def]}]

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [], []]
  end

  test_each "uses the given SQL expressions to filter which rows are seen in the table for tables with an only attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[4, nil, "foo"],
                      [5, nil,   nil]]

    send_handshake_commands(filters: {"footbl" => {"where_conditions" => "col1 BETWEEN 4 AND 7"}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA, [{"tables" => [footbl_def, secondtbl_def]}]

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [4], [5]] # note 5 is the last actual key that's present, even though the filter allowed up to 7

    send_command   Commands::HASH, ["footbl", [], [4], 1000]
    expect_command Commands::HASH,
                   ["footbl", [], [4], 1000, 1, hash_of(@filtered_rows[0..0])]

    send_command   Commands::ROWS, ["footbl", [], []]
    expect_command Commands::ROWS,
                   ["footbl", [], []],
                   @filtered_rows[0],
                   @filtered_rows[1]

    send_command   Commands::ROWS, ["footbl", [4], []]
    expect_command Commands::ROWS,
                   ["footbl", [4], []],
                   @filtered_rows[1]

    send_command   Commands::ROWS, ["footbl", [], [5]]
    expect_command Commands::ROWS,
                   ["footbl", [], [5]],
                   @filtered_rows[0],
                   @filtered_rows[1]
  end

  test_each "uses the given SQL expressions to filter column values in hash and rows commands for tables with a replace attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longerstr')"
    @filtered_rows = [[2,   6,      "testx"],
                      [4,   7,       "foox"],
                      [5, nil,    "default"],
                      [8,  17, "longerstrx"]]

    send_handshake_commands(filters: {"footbl" => {"filter_expressions" => {"another_col" => "col1 + CHAR_LENGTH(col3)", "col3" => "COALESCE(col3 || 'x', 'default')"}}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA, [{"tables" => [footbl_def, secondtbl_def]}]

    send_command   Commands::HASH, ["footbl", [], [2], 1000]
    expect_command Commands::HASH,
                   ["footbl", [], [2], 1000, 1, hash_of(@filtered_rows[0..0])]

    send_command   Commands::ROWS, ["footbl", [], []]
    expect_command Commands::ROWS,
                   ["footbl", [], []],
                   @filtered_rows[0],
                   @filtered_rows[1],
                   @filtered_rows[2],
                   @filtered_rows[3]
  end

  test_each "raises an error if told to replace column values in a primary key column" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[2,   6,       "testx"],
                      [4,   7,        "foox"],
                      [5, nil,     "default"],
                      [8,  18, "longer strx"]]

    send_handshake_commands(filters: {"footbl" => {"filter_expressions" => {"col1" => "col1*2"}}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA, [{"tables" => [footbl_def, secondtbl_def], "errors" => ["Can't replace values in column 'col1' table 'footbl' because it is used in the primary key"]}]
  end

  test_each "applies both column filters and row filters if given" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[4,   7,     "foo"],
                      [5, nil, "default"]]

    send_handshake_commands(filters: {"footbl" => {"where_conditions" => "col1 BETWEEN 4 AND 7", "filter_expressions" => {"another_col" => "col1 + CHAR_LENGTH(col3)", "col3" => "COALESCE(col3, 'default')"}}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA, [{"tables" => [footbl_def, secondtbl_def]}]

    send_command   Commands::HASH, ["footbl", [], [4], 1000]
    expect_command Commands::HASH,
                   ["footbl", [], [4], 1000, 1, hash_of(@filtered_rows[0..0])]

    send_command   Commands::ROWS, ["footbl", [], []]
    expect_command Commands::ROWS,
                   ["footbl", [], []],
                   @filtered_rows[0],
                   @filtered_rows[1]
  end

  test_each "accepts and applies filters given after the schema instead of before, for protocol version 7 and below" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[4,   7,     "foo"],
                      [5, nil, "default"]]

    send_handshake_commands(protocol_version: 7)

    send_command   Commands::FILTERS,
                   [{"footbl" => {"where_conditions" => "col1 BETWEEN 4 AND 7", "filter_expressions" => {"another_col" => "col1 + CHAR_LENGTH(col3)", "col3" => "COALESCE(col3, 'default')"}}}]
    expect_command Commands::FILTERS

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA, [{"tables" => [footbl_def_v7]}]

    send_command   Commands::HASH, ["footbl", [], [4], 1000]
    expect_command Commands::HASH,
                   ["footbl", [], [4], 1000, 1, hash_of(@filtered_rows[0..0])]

    send_command   Commands::ROWS, ["footbl", [], []]
    expect_command Commands::ROWS,
                   ["footbl", [], []],
                   @filtered_rows[0],
                   @filtered_rows[1]
  end
end
