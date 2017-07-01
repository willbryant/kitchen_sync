require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

require 'tempfile'

class FilterFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def with_filter_file(contents)
    file = Tempfile.new('filter')
    file.write(contents)
    file.close
    begin
      program_env['ENDPOINT_FILTERS_FILE'] = file.path
      yield
    ensure
      file.unlink
    end
  end

  test_each "sends the empty range immediately for tables with a clear attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    with_filter_file("footbl: clear \n") do # nonsignificant whitespace at the end should be ignored
      send_handshake_commands

      send_command   Commands::OPEN, ["footbl"]
      expect_command Commands::ROWS,
                     [[], []]
    end
  end

  test_each "sees an empty table for the RANGE command for tables with a clear attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    with_filter_file("footbl: clear \n") do # nonsignificant whitespace at the end should be ignored
      send_handshake_commands

      send_command   Commands::RANGE, ["footbl"]
      expect_command Commands::RANGE,
                     ["footbl", [], []]
    end
  end

  test_each "uses the given SQL expressions to filter which rows are seen in the table for tables with an only attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[4, nil, "foo"],
                      [5, nil,   nil]]

    with_filter_file("footbl:\n  only: col1 BETWEEN 4 AND 7") do
      send_handshake_commands

      send_command   Commands::RANGE, ["footbl"]
      expect_command Commands::RANGE,
                     ["footbl", [4], [5]] # note 5 is the last actual key that's present, even though the filter allowed up to 7

      send_command   Commands::HASH, ["footbl", [], [4], 1000]
      expect_command Commands::HASH,
                     ["footbl", [], [4], 1, hash_of(@filtered_rows[0..0])]

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
  end

  test_each "uses the given SQL expressions to filter column values in hash and rows commands for tables with a replace attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[2,   6,       "testx"],
                      [4,   7,        "foox"],
                      [5, nil,     "default"],
                      [8,  18, "longer strx"]]

    with_filter_file("footbl:\n  replace:\n    another_col: col1 + CHAR_LENGTH(col3)\n    col3: COALESCE(col3 || 'x', 'default')") do
      send_handshake_commands

      send_command   Commands::HASH, ["footbl", [], [2], 1000]
      expect_command Commands::HASH,
                     ["footbl", [], [2], 1, hash_of(@filtered_rows[0..0])]

      send_command   Commands::ROWS, ["footbl", [], []]
      expect_command Commands::ROWS,
                     ["footbl", [], []],
                     @filtered_rows[0],
                     @filtered_rows[1],
                     @filtered_rows[2],
                     @filtered_rows[3]
    end
  end

  test_each "applies both column filters and row filters if given" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [[4,   7,     "foo"],
                      [5, nil, "default"]]

    with_filter_file("footbl:\n  replace:\n    another_col: col1 + CHAR_LENGTH(col3)\n    col3: COALESCE(col3, 'default')\n  only: col1 BETWEEN 4 AND 7") do
      send_handshake_commands
      send_command   Commands::HASH, ["footbl", [], [4], 1000]
      expect_command Commands::HASH,
                     ["footbl", [], [4], 1, hash_of(@filtered_rows[0..0])]

      send_command   Commands::ROWS, ["footbl", [], []]
      expect_command Commands::ROWS,
                     ["footbl", [], []],
                     @filtered_rows[0],
                     @filtered_rows[1]
    end
  end
end
