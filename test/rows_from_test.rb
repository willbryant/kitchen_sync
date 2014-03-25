require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class RowsFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "returns an empty array if there are no such rows, extending the range to the end of the table if there are no later rows" do
    create_some_tables
    send_handshake_commands

    send_command   Commands::OPEN, "footbl"
    expect_command Commands::ROWS,
                   [[], []]

    send_command   Commands::ROWS, ["0"], ["0"]
    expect_command Commands::ROWS,
                   [["0"], ["0"]]

    send_command   Commands::ROWS, ["-1"], ["0"]
    expect_command Commands::ROWS,
                   [["-1"], ["0"]]

    send_command   Commands::ROWS, ["10"], ["11"]
    expect_command Commands::ROWS,
                   [["10"], ["11"]]

    send_command   Commands::OPEN, "secondtbl"
    expect_command Commands::ROWS,
                   [[], []]
    send_command   Commands::ROWS, ["aa", "0"], ["ab", "0"]
    expect_command Commands::ROWS,
                   [["aa", "0"], ["ab", "0"]]
  end

  test_each "returns all the rows whose key is greater than the first argument and not greater than the last argument" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @rows = [["2",  "10",       "test"],
             ["4",   nil,        "foo"],
             ["5",   nil,          nil],
             ["8",  "-1", "longer str"]]
    @keys = @rows.collect {|row| [row[0]]}
    send_handshake_commands

    send_command   Commands::OPEN, "footbl"
    expect_command Commands::HASH_NEXT,
                   [[], ["2"], hash_of(@rows[0..0])]

    send_command   Commands::ROWS, ["1"], ["2"]
    expect_command Commands::ROWS,
                   [["1"], ["2"]],
                   @rows[0]

    send_command   Commands::ROWS, ["1"], ["2"] # same request
    expect_command Commands::ROWS,
                   [["1"], ["2"]],
                   @rows[0]

    send_command   Commands::ROWS, ["0"], ["2"] # different request, but same data matched
    expect_command Commands::ROWS,
                   [["0"], ["2"]],
                   @rows[0]

    send_command   Commands::ROWS, ["1"], ["3"] # ibid
    expect_command Commands::ROWS,
                   [["1"], ["3"]],
                   @rows[0]

    send_command   Commands::ROWS, ["3"], ["4"] # null numbers
    expect_command Commands::ROWS,
                   [["3"], ["4"]],
                   @rows[1]

    send_command   Commands::ROWS, ["4"], ["5"] # null strings
    expect_command Commands::ROWS,
                   [["4"], ["5"]],
                   @rows[2]

    send_command   Commands::ROWS, ["5"], ["9"] # negative numbers
    expect_command Commands::ROWS,
                   [["5"], ["9"]],
                   @rows[3]

    send_command   Commands::ROWS, ["0"], ["10"]
    expect_command Commands::ROWS,
                   [["0"], ["10"]],
                   *@rows
  end

  test_each "starts from the first row if an empty array is given as the first argument" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 3, 'foo'), (4, 5, 'bar')"
    @rows = [["2", "3", "foo"],
             ["4", "5", "bar"]]
    @keys = @rows.collect {|row| [row[0]]}
    send_handshake_commands

    send_command   Commands::OPEN, "footbl"
    expect_command Commands::HASH_NEXT,
                   [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::ROWS, [], @keys[0]
    expect_command Commands::ROWS,
                   [[], @keys[0]],
                   @rows[0]

    send_command   Commands::ROWS, [], @keys[1]
    expect_command Commands::ROWS,
                   [[], @keys[1]],
                   @rows[0],
                   @rows[1]

    send_command   Commands::ROWS, [], ["10"]
    expect_command Commands::ROWS,
                   [[], ["10"]],
                   @rows[0],
                   @rows[1]
  end

  test_each "supports composite keys" do
    create_some_tables
    execute "INSERT INTO secondtbl VALUES (2349174, 'xy', 1, 2), (968116383, 'aa', 9, 9), (100, 'aa', 100, 100), (363401169, 'ab', 20, 340)"
    send_handshake_commands

    # note when reading these that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated

    send_command   Commands::OPEN, "secondtbl"
    expect_command Commands::HASH_NEXT,
                   [[], ["aa", "100"], hash_of([["100", "aa", "100", "100"]])]

    send_command   Commands::ROWS, ["aa", "1"], ["zz", "2147483647"]
    expect_command Commands::ROWS,
                   [["aa", "1"], ["zz", "2147483647"]],
                   [      "100", "aa", "100", "100"], # first because aa is the first term in the key, then 100 the next
                   ["968116383", "aa",   "9",   "9"],
                   ["363401169", "ab",  "20", "340"],
                   [  "2349174", "xy",   "1",   "2"]

    send_command   Commands::ROWS, ["aa", "101"], ["aa", "1000000000"]
    expect_command Commands::ROWS,
                   [["aa", "101"], ["aa", "1000000000"]],
                   ["968116383", "aa", "9", "9"]

    send_command   Commands::ROWS, ["aa", "100"], ["aa", "1000000000"]
    expect_command Commands::ROWS,
                   [["aa", "100"], ["aa", "1000000000"]],
                   ["968116383", "aa", "9", "9"]

    send_command   Commands::ROWS, ["ww", "1"], ["zz", "1"]
    expect_command Commands::ROWS,
                   [["ww", "1"], ["zz", "1"]],
                   ["2349174", "xy", "1", "2"]

    send_command   Commands::ROWS, ["xy", "1"], ["xy", "10000000"]
    expect_command Commands::ROWS,
                   [["xy", "1"], ["xy", "10000000"]],
                   ["2349174", "xy", "1", "2"]
  end

  test_each "supports reserved-word column names" do
    clear_schema
    create_reservedtbl
    send_handshake_commands

    send_command   Commands::OPEN, "reservedtbl"
    expect_command Commands::ROWS,
                   [[], []]

    send_command   Commands::ROWS, [], []
    expect_command Commands::ROWS,
                   [[], []]
  end
end
