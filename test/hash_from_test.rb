require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

require 'openssl'

class HashFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def hash_of(rows)
    md5 = OpenSSL::Digest::MD5.new
    md5.digest(rows.collect(&:to_msgpack).join)
  end

  test_each "returns the hash of an empty array if there are no such rows" do
    create_some_tables
    send_protocol_command

    assert_equal(hash_of([]), send_command("hash", "footbl", ["0"], 1).force_encoding("ASCII-8BIT"))
    assert_equal(hash_of([]), send_command("hash", "footbl", ["-1"], 1).force_encoding("ASCII-8BIT"))
    assert_equal(hash_of([]), send_command("hash", "footbl", ["10"], 1).force_encoding("ASCII-8BIT"))
    assert_equal(hash_of([]), send_command("hash", "secondtbl", ["aa", "0"], 1).force_encoding("ASCII-8BIT"))
  end

  test_each "returns the given data if requested" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (1, 10, 'test'), (3, NULL, 'foo'), (4, NULL, NULL), (8, -1, 'longer str')"
    send_protocol_command

    assert_equal(hash_of([["1", "10", "test"      ]]), send_command("hash", "footbl", ["1"], 1).force_encoding("ASCII-8BIT"))
    assert_equal(hash_of([["1", "10", "test"      ]]), send_command("hash", "footbl", ["1"], 1).force_encoding("ASCII-8BIT")) # same request
    assert_equal(hash_of([["1", "10", "test"      ]]), send_command("hash", "footbl", ["0"], 1).force_encoding("ASCII-8BIT")) # different request, but same data matched
    assert_equal(hash_of([["1", "10", "test"      ]]), send_command("hash", "footbl", ["1"], 1).force_encoding("ASCII-8BIT")) # ibid

    assert_equal(hash_of([["3",  nil, "foo"       ]]), send_command("hash", "footbl", ["3"], 1).force_encoding("ASCII-8BIT")) # null numbers
    assert_equal(hash_of([["4",  nil, nil         ]]), send_command("hash", "footbl", ["4"], 1).force_encoding("ASCII-8BIT")) # null strings
    assert_equal(hash_of([["8", "-1", "longer str"]]), send_command("hash", "footbl", ["5"], 1).force_encoding("ASCII-8BIT")) # negative numbers

    assert_equal(hash_of([["1", "10", "test"      ],
                          ["3",  nil, "foo"       ],
                          ["4",  nil, nil         ],
                          ["8", "-1", "longer str"]]),
                 send_command("hash", "footbl", ["0"], 100).force_encoding("ASCII-8BIT"))

    assert_equal(hash_of([["1", "10", "test"      ],
                          ["3",  nil, "foo"       ],
                          ["4",  nil, nil         ]]),
                 send_command("hash", "footbl", ["0"], 3).force_encoding("ASCII-8BIT"))

    assert_equal(hash_of([["3",  nil, "foo"       ],
                          ["4",  nil, nil         ]]),
                 send_command("hash", "footbl", ["3"], 2).force_encoding("ASCII-8BIT"))
  end

  test_each "supports composite keys" do
    create_some_tables
    execute "INSERT INTO secondtbl VALUES (2349174, 'xy', 1, 2), (968116383, 'aa', 9, 9), (100, 'aa', 100, 100), (363401169, 'ab', 20, 340)"
    send_protocol_command

    # note when reading these that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated

    assert_equal(hash_of([[      "100", "aa", "100", "100"], # first because aa is the first term in the key, then 100 the next
                          ["968116383", "aa",   "9",   "9"],
                          ["363401169", "ab",  "20", "340"],
                          [  "2349174", "xy",   "1",   "2"]]),
                 send_command("hash", "secondtbl", ["aa", "1"], 100).force_encoding("ASCII-8BIT"))

    assert_equal(hash_of([[      "100", "aa", "100", "100"],
                          ["968116383", "aa",   "9",   "9"],
                          ["363401169", "ab",  "20", "340"]]),
                 send_command("hash", "secondtbl", ["aa", "1"], 3).force_encoding("ASCII-8BIT"))

    assert_equal(hash_of([["968116383", "aa",   "9",   "9"],
                          ["363401169", "ab",  "20", "340"],
                          [  "2349174", "xy",   "1",   "2"]]),
                 send_command("hash", "secondtbl", ["aa", "101"], 100).force_encoding("ASCII-8BIT"))

    assert_equal(hash_of([["968116383", "aa", "9", "9"]]),
                 send_command("hash", "secondtbl", ["aa", "101"], 1).force_encoding("ASCII-8BIT"))
    assert_equal(hash_of([["2349174", "xy", "1", "2"]]),
                 send_command("hash", "secondtbl", ["ww", "1"], 1).force_encoding("ASCII-8BIT"))
    assert_equal(hash_of([["2349174", "xy", "1", "2"]]),
                 send_command("hash", "secondtbl", ["xy", "1"], 1).force_encoding("ASCII-8BIT"))
  end
end
