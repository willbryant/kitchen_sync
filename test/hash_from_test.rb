require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class HashFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def send_hash_command(*args)
    send_command("hash", *args).tap {|results| results[0] = results[0].force_encoding("ASCII-8BIT")}
  end

  test_each "returns the hash of an empty array if there are no such rows, and an empty primary key" do
    create_some_tables
    send_protocol_command

    assert_equal([hash_of([]), []], send_hash_command("footbl", ["0"], 1))
    assert_equal([hash_of([]), []], send_hash_command("footbl", ["-1"], 1))
    assert_equal([hash_of([]), []], send_hash_command("footbl", ["10"], 1))
    assert_equal([hash_of([]), []], send_hash_command("secondtbl", ["aa", "0"], 1))
  end

  test_each "returns the hash of all the rows whose key is greater than the first argument and not greater than the last argument, and the primary key of the last row" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    send_protocol_command

    assert_equal([hash_of([["2", "10", "test"      ]]), ["2"]], send_hash_command("footbl", ["1"], 1))
    assert_equal([hash_of([["2", "10", "test"      ]]), ["2"]], send_hash_command("footbl", ["1"], 1)) # same request
    assert_equal([hash_of([["2", "10", "test"      ]]), ["2"]], send_hash_command("footbl", ["0"], 1)) # different request, but same data matched
    assert_equal([hash_of([["2", "10", "test"      ]]), ["2"]], send_hash_command("footbl", ["1"], 1)) # ibid

    assert_equal([hash_of([["4",  nil, "foo"       ]]), ["4"]], send_hash_command("footbl", ["3"], 1)) # null numbers
    assert_equal([hash_of([["5",  nil, nil         ]]), ["5"]], send_hash_command("footbl", ["4"], 1)) # null strings
    assert_equal([hash_of([["8", "-1", "longer str"]]), ["8"]], send_hash_command("footbl", ["5"], 1)) # negative numbers

    assert_equal([hash_of([["2", "10", "test"      ],
                           ["4",  nil, "foo"       ],
                           ["5",  nil, nil         ],
                           ["8", "-1", "longer str"]]),
                  ["8"]],
                 send_hash_command("footbl", ["0"], 100))

    assert_equal([hash_of([["2", "10", "test"      ],
                           ["4",  nil, "foo"       ],
                           ["5",  nil, nil         ]]),
                  ["5"]],
                 send_hash_command("footbl", ["0"], 3))

    assert_equal([hash_of([["4",  nil, "foo"       ],
                           ["5",  nil, nil         ]]),
                  ["5"]],
                 send_hash_command("footbl", ["3"], 2))
  end

  test_each "starts from the first row if an empty array is given as the first argument" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 3, 'foo'), (4, 5, 'bar')"
    send_protocol_command

    assert_equal([hash_of([["2", "3", "foo"]]), ["2"]], send_hash_command("footbl", [], 1))
    assert_equal([hash_of([["2", "3", "foo"], ["4", "5", "bar"]]), ["4"]], send_hash_command("footbl", [], 2))
    assert_equal([hash_of([["2", "3", "foo"], ["4", "5", "bar"]]), ["4"]], send_hash_command("footbl", [], 10))
  end

  test_each "supports composite keys" do
    create_some_tables
    execute "INSERT INTO secondtbl VALUES (2349174, 'xy', 1, 2), (968116383, 'aa', 9, 9), (100, 'aa', 100, 100), (363401169, 'ab', 20, 340)"
    send_protocol_command

    # note when reading these that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated

    assert_equal([hash_of([[      "100", "aa", "100", "100"], # first because aa is the first term in the key, then 100 the next
                           ["968116383", "aa",   "9",   "9"],
                           ["363401169", "ab",  "20", "340"],
                           [  "2349174", "xy",   "1",   "2"]]),
                  ["xy", "2349174"]], # note in primary key order, not column order
                 send_hash_command("secondtbl", ["aa", "1"], 100))

    assert_equal([hash_of([[      "100", "aa", "100", "100"],
                           ["968116383", "aa",   "9",   "9"],
                           ["363401169", "ab",  "20", "340"]]),
                  ["ab", "363401169"]],
                 send_hash_command("secondtbl", ["aa", "1"], 3))

    assert_equal([hash_of([["968116383", "aa",   "9",   "9"],
                           ["363401169", "ab",  "20", "340"],
                           [  "2349174", "xy",   "1",   "2"]]),
                  ["xy", "2349174"]],
                 send_hash_command("secondtbl", ["aa", "101"], 100))

    assert_equal([hash_of([["968116383", "aa", "9", "9"]]), ["aa", "968116383"]],
                 send_hash_command("secondtbl", ["aa", "101"], 1))
    assert_equal([hash_of([["2349174", "xy", "1", "2"]]), ["xy", "2349174"]],
                 send_hash_command("secondtbl", ["ww", "1"], 1))
    assert_equal([hash_of([["2349174", "xy", "1", "2"]]), ["xy", "2349174"]],
                 send_hash_command("secondtbl", ["xy", "1"], 1))
  end
end
