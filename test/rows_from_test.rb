require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class RowsFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def send_rows_command(*args)
    # rather than return a single array containing all the requested rows, which would mean the rows would
    # have to be buffered and counted before sending them, the rows command returns multiple packed objects,
    # terminating the resultset with a nil object (which is unambiguous since the rows are returned as arrays).
    rows = []
    result = send_command("rows", *args)
    while result
      rows << result
      result = unpacker.read
    end
    rows
  end

  test_each "returns an empty array if there are no such rows" do
    create_some_tables
    send_protocol_command

    assert_equal([], send_rows_command("footbl", ["0"], ["0"]))
    assert_equal([], send_rows_command("footbl", ["-1"], ["0"]))
    assert_equal([], send_rows_command("footbl", ["10"], ["11"]))
    assert_equal([], send_rows_command("secondtbl", ["aa", "0"], ["aa", "0"]))
  end

  test_each "returns the given data if requested" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (1, 10, 'test'), (3, NULL, 'foo'), (4, NULL, NULL), (8, -1, 'longer str')"
    send_protocol_command

    assert_equal([["1", "10", "test"      ]], send_rows_command("footbl", ["1"], ["1"]))
    assert_equal([["1", "10", "test"      ]], send_rows_command("footbl", ["1"], ["1"])) # same request
    assert_equal([["1", "10", "test"      ]], send_rows_command("footbl", ["0"], ["1"])) # different request, but same data matched
    assert_equal([["1", "10", "test"      ]], send_rows_command("footbl", ["1"], ["2"])) # ibid

    assert_equal([["3",  nil, "foo"       ]], send_rows_command("footbl", ["3"], ["3"])) # null numbers
    assert_equal([["4",  nil, nil         ]], send_rows_command("footbl", ["4"], ["4"])) # null strings
    assert_equal([["8", "-1", "longer str"]], send_rows_command("footbl", ["5"], ["9"])) # negative numbers

    assert_equal([["1", "10", "test"      ],
                  ["3",  nil, "foo"       ],
                  ["4",  nil, nil         ],
                  ["8", "-1", "longer str"]],
                 send_rows_command("footbl", ["0"], ["10"]))
  end

  test_each "supports composite keys" do
    create_some_tables
    execute "INSERT INTO secondtbl VALUES (2349174, 'xy', 1, 2), (968116383, 'aa', 9, 9), (100, 'aa', 100, 100), (363401169, 'ab', 20, 340)"
    send_protocol_command

    # note when reading these that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated

    assert_equal([[      "100", "aa", "100", "100"], # first because aa is the first term in the key, then 100 the next
                  ["968116383", "aa",   "9",   "9"],
                  ["363401169", "ab",  "20", "340"],
                  [  "2349174", "xy",   "1",   "2"]],
                 send_rows_command("secondtbl", ["aa", "1"], ["zz", "2147483647"]))

    assert_equal([["968116383", "aa", "9", "9"]],
                 send_rows_command("secondtbl", ["aa", "101"], ["aa", "1000000000"]))
    assert_equal([["2349174", "xy", "1", "2"]],
                 send_rows_command("secondtbl", ["ww", "1"], ["zz", "1"]))
    assert_equal([["2349174", "xy", "1", "2"]],
                 send_rows_command("secondtbl", ["xy", "1"], ["xy", "10000000"]))
  end
end
