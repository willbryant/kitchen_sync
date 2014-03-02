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
      program_args << file.path
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

      assert_equal [Commands::ROWS, [], []],
       send_command(Commands::OPEN, "footbl")
      assert_equal [], unpack_next
    end
  end

  test_each "uses the given SQL expressions to filter which rows are seen in the table for tables with an only attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [["4", nil, "foo"],
                      ["5", nil,   nil]]
    with_filter_file("footbl:\n  only: col1 BETWEEN 4 AND 7") do
      send_handshake_commands

      assert_equal [Commands::HASH_NEXT, [], ["4"], hash_of(@filtered_rows[0..0])],
       send_command(Commands::OPEN, "footbl")

      assert_equal [Commands::ROWS, [], []],
       send_command(Commands::ROWS, [], [])
      assert_equal @filtered_rows[0], unpack_next
      assert_equal @filtered_rows[1], unpack_next
      assert_equal                [], unpack_next

      assert_equal [Commands::ROWS, ["4"], []],
       send_command(Commands::ROWS, ["4"], [])
      assert_equal @filtered_rows[1], unpack_next
      assert_equal                [], unpack_next

      assert_equal [Commands::ROWS, [], ["5"]],
       send_command(Commands::ROWS, [], ["5"])
      assert_equal @filtered_rows[0], unpack_next
      assert_equal @filtered_rows[1], unpack_next
      assert_equal                [], unpack_next
    end
  end

  test_each "uses the given SQL expressions to filter column values in hash and rows commands for tables with a replace attribute" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [["2",  "6",       "test"],
                      ["4",  "7",        "foo"],
                      ["5",  nil,    "default"],
                      ["8", "18", "longer str"]]
    with_filter_file("footbl:\n  replace:\n    another_col: col1 + CHAR_LENGTH(col3)\n    col3: COALESCE(col3, 'default')") do
      send_handshake_commands

      assert_equal [Commands::HASH_NEXT, [], ["2"], hash_of(@filtered_rows[0..0])],
       send_command(Commands::OPEN, "footbl")

      assert_equal [Commands::ROWS, [], []],
       send_command(Commands::ROWS, [], [])
      assert_equal @filtered_rows[0], unpack_next
      assert_equal @filtered_rows[1], unpack_next
      assert_equal @filtered_rows[2], unpack_next
      assert_equal @filtered_rows[3], unpack_next
      assert_equal                [], unpack_next
    end
  end

  test_each "applies both column filters and row filters if given" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @filtered_rows = [["4",  "7",     "foo"],
                      ["5",  nil, "default"]]
    with_filter_file("footbl:\n  replace:\n    another_col: col1 + CHAR_LENGTH(col3)\n    col3: COALESCE(col3, 'default')\n  only: col1 BETWEEN 4 AND 7") do
      send_handshake_commands
      assert_equal [Commands::HASH_NEXT, [], ["4"], hash_of(@filtered_rows[0..0])],
       send_command(Commands::OPEN, "footbl")

      assert_equal [Commands::ROWS, [], []],
       send_command(Commands::ROWS, [], [])
      assert_equal @filtered_rows[0], unpack_next
      assert_equal @filtered_rows[1], unpack_next
      assert_equal                [], unpack_next
    end
  end
end
