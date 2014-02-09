require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class RowsFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def assert_next_hash_command(table, prev_key)
    command = unpack_next
    assert_equal Commands::HASH, command[0]
    assert_equal prev_key, command[1]
    assert_not_equal prev_key, command[2]
  end

  test_each "returns an empty array if there are no such rows, extending the range to the end of the table if there are no later rows" do
    create_some_tables
    send_handshake_commands

    assert_equal nil, send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS, ["0"], []],
     send_command(Commands::ROWS, ["0"], ["0"])
    assert_equal [], unpack_next

    assert_equal [Commands::ROWS, ["-1"], []],
     send_command(Commands::ROWS, ["-1"], ["0"])
    assert_equal [], unpack_next

    assert_equal [Commands::ROWS, ["10"], []],
     send_command(Commands::ROWS, ["10"], ["11"])
    assert_equal [], unpack_next

    assert_equal nil, send_command(Commands::OPEN, "secondtbl")
    assert_equal [Commands::ROWS, ["aa", "0"], []],
     send_command(Commands::ROWS, ["aa", "0"], ["aa", "0"])
    assert_equal [], unpack_next
  end

  test_each "returns all the rows whose key is greater than the first argument and not greater than the last argument" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    send_handshake_commands

    assert_equal nil, send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS, ["1"], ["2"]],
     send_command(Commands::ROWS, ["1"], ["2"])
    assert_equal ["2", "10", "test"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["2"])

    assert_equal [Commands::ROWS, ["1"], ["2"]],
     send_command(Commands::ROWS, ["1"], ["2"]) # same request
    assert_equal ["2", "10", "test"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["2"])

    assert_equal [Commands::ROWS, ["0"], ["2"]],
     send_command(Commands::ROWS, ["0"], ["2"]) # different request, but same data matched
    assert_equal ["2", "10", "test"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["2"])

    assert_equal [Commands::ROWS, ["1"], ["3"]],
     send_command(Commands::ROWS, ["1"], ["3"]) # ibid
    assert_equal ["2", "10", "test"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["3"])

    assert_equal [Commands::ROWS, ["3"], ["4"]],
     send_command(Commands::ROWS, ["3"], ["4"]) # null numbers
    assert_equal ["4",  nil, "foo"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["4"])

    assert_equal [Commands::ROWS, ["4"], ["5"]],
     send_command(Commands::ROWS, ["4"], ["5"]) # null strings
    assert_equal ["5",  nil, nil], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["5"])

    assert_equal [Commands::ROWS, ["5"], []],
     send_command(Commands::ROWS, ["5"], ["9"]) # negative numbers
    assert_equal ["8", "-1", "longer str"], unpack_next
    assert_equal [], unpack_next
    # no subsequent hash command, since the returned range was extended to the end of the table

    assert_equal [Commands::ROWS, ["0"], []],
     send_command(Commands::ROWS, ["0"], ["10"])
    assert_equal ["2", "10", "test"      ], unpack_next
    assert_equal ["4",  nil, "foo"       ], unpack_next
    assert_equal ["5",  nil, nil         ], unpack_next
    assert_equal ["8", "-1", "longer str"], unpack_next
    assert_equal [], unpack_next
    # no subsequent hash command, since the returned range was extended to the end of the table
  end

  test_each "starts from the first row if an empty array is given as the first argument" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 3, 'foo'), (4, 5, 'bar')"
    send_handshake_commands

    assert_equal nil, send_command(Commands::OPEN, "footbl")
    assert_equal [Commands::ROWS, [], ["2"]],
     send_command(Commands::ROWS, [], ["2"])
    assert_equal ["2", "3", "foo"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("footbl", ["2"])

    assert_equal [Commands::ROWS, [], []],
     send_command(Commands::ROWS, [], ["4"])
    assert_equal ["2", "3", "foo"], unpack_next
    assert_equal ["4", "5", "bar"], unpack_next
    assert_equal [], unpack_next

    assert_equal [Commands::ROWS, [], []],
     send_command(Commands::ROWS, [], ["10"])
    assert_equal ["2", "3", "foo"], unpack_next
    assert_equal ["4", "5", "bar"], unpack_next
    assert_equal [], unpack_next
  end

  test_each "supports composite keys" do
    create_some_tables
    execute "INSERT INTO secondtbl VALUES (2349174, 'xy', 1, 2), (968116383, 'aa', 9, 9), (100, 'aa', 100, 100), (363401169, 'ab', 20, 340)"
    send_handshake_commands

    # note when reading these that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated

    assert_equal nil, send_command(Commands::OPEN, "secondtbl")
    assert_equal [Commands::ROWS, ["aa", "1"], []],
     send_command(Commands::ROWS, ["aa", "1"], ["zz", "2147483647"])
    assert_equal [      "100", "aa", "100", "100"], unpack_next # first because aa is the first term in the key, then 100 the next
    assert_equal ["968116383", "aa",   "9",   "9"], unpack_next
    assert_equal ["363401169", "ab",  "20", "340"], unpack_next
    assert_equal [  "2349174", "xy",   "1",   "2"], unpack_next
    assert_equal [], unpack_next

    assert_equal [Commands::ROWS, ["aa", "101"], ["aa", "1000000000"]],
     send_command(Commands::ROWS, ["aa", "101"], ["aa", "1000000000"])
    assert_equal ["968116383", "aa", "9", "9"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("secondtbl", ["aa", "1000000000"])

    assert_equal [Commands::ROWS, ["aa", "100"], ["aa", "1000000000"]],
     send_command(Commands::ROWS, ["aa", "100"], ["aa", "1000000000"])
    assert_equal ["968116383", "aa", "9", "9"], unpack_next
    assert_equal [], unpack_next
    assert_next_hash_command("secondtbl", ["aa", "1000000000"])

    assert_equal [Commands::ROWS, ["ww", "1"], []],
     send_command(Commands::ROWS, ["ww", "1"], ["zz", "1"])
    assert_equal ["2349174", "xy", "1", "2"], unpack_next
    assert_equal [], unpack_next

    assert_equal [Commands::ROWS, ["xy", "1"], []],
     send_command(Commands::ROWS, ["xy", "1"], ["xy", "10000000"])
    assert_equal ["2349174", "xy", "1", "2"], unpack_next
    assert_equal [], unpack_next
  end

  test_each "supports reserved-word column names" do
    clear_schema
    create_reservedtbl
    send_handshake_commands

    assert_equal nil, send_command(Commands::OPEN, "reservedtbl")
    assert_equal [Commands::ROWS, [], []],
     send_command(Commands::ROWS, [], [])
    assert_equal [], unpack_next
  end
end
