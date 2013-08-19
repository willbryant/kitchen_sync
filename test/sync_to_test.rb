require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SyncToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def setup
    # checking how protocol versions are handled is covered in protocol_versions_test; here we just need to get past that to get on to the commands we want to test
    expects(:protocol).with(CURRENT_PROTOCOL_VERSION).returns([CURRENT_PROTOCOL_VERSION])
  end

  test_each "is immediately asked for all rows if the other end has an empty table, and no change is made" do
    clear_schema
    create_footbl

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:rows).with("footbl", [], []).returns([[]])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "it moves to the key range after the last key returned if we return a matching hash, and is asked for all remaining rows if the other end has no more rows" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test')"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).in_sequence.with("footbl",    [], ["2"]).returns([hash_and_count_of([["2", "10", "test"]])])
    expects(:rows).in_sequence.with("footbl", ["2"],    []).returns([[]])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "requests hashes for twice as many each iteration if we continue to return matching hashes" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL)"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).in_sequence.with("footbl",    [], ["2"]).returns([hash_and_count_of([["2", "10", "test"]])])
    expects(:hash).in_sequence.with("footbl", ["2"], ["5"]).returns([hash_and_count_of([["3", nil, "foo"], ["5", nil, nil]])])
    expects(:rows).in_sequence.with("footbl", ["5"],    []).returns([[]])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"],
                  ["3",  nil,  "foo"],
                  ["5",  nil,    nil]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "is asked for the hash of the rows up to the first row's key, and clears the table if we indicate there's no rows straight away" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).with("footbl", [], ["2"]).returns([hash_and_count_of([])])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "clears the table if we indicate there's no rows straight away" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).in_sequence.with("footbl", [], ["2"]).returns([hash_and_count_of([])])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "clears the remainder of the table if we indicate there's no rows after an initial match" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).in_sequence.with("footbl",    [], ["2"]).returns([hash_and_count_of([["2", "10", "test"]])])
    expects(:hash).in_sequence.with("footbl", ["2"], ["5"]).returns([hash_and_count_of([])])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "clears the remainder of the table if we indicate there's no rows after multiple matches" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).in_sequence.with("footbl",    [], ["2"]).returns([hash_and_count_of([["2", "10", "test"]])])
    expects(:hash).in_sequence.with("footbl", ["2"], ["5"]).returns([hash_and_count_of([["3", nil, "foo"], ["5", nil, nil]])])
    expects(:hash).in_sequence.with("footbl", ["5"], ["8"]).returns([hash_and_count_of([])])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"],
                  ["3",  nil,  "foo"],
                  ["5",  nil,    nil]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "is asked for the row if we return a different hash straight away, after which it updates the table, and carries on from the last key" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 1, 'different'), (3, NULL, 'foo')"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).in_sequence.with("footbl", [], ["2"]).returns([hash_and_count_of([["2", "10", "test"]])])
    # the rows command returns [multiple results rather than one array of arrays, to avoid the need to know the number of rows in advanc]e
    expects(:rows).in_sequence.with("footbl", [], ["2"]).returns([["2", "10", "test"], []])
    expects(:hash).in_sequence.with("footbl", ["2"], ["3"]).returns([hash_and_count_of([["3", nil, "foo"]])])
    expects(:rows).in_sequence.with("footbl", ["3"], []).returns([[]])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"],
                  ["3",  nil,  "foo"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles data after nil elements" do
    clear_schema
    create_footbl
    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:rows).in_sequence.with("footbl", [], []).returns([["2", nil, nil], ["3",  nil,  "foo"], []])
    expects(:quit)
    receive_commands

    assert_equal [["2", nil,   nil],
                  ["3", nil, "foo"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles hashing medium values" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (1, '#{'a'*16*1024}')"

    medium_row = ["1", "a"*16*1024]

    expects(:schema).with().returns([{"tables" => [texttbl_def]}])
    expects(:hash).in_sequence.with("texttbl", [], ["1"]).returns([hash_and_count_of([medium_row])])
    expects(:rows).in_sequence.with("texttbl", ["1"], []).returns([[]])
    expects(:quit)
    receive_commands

    assert_equal [medium_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving medium values" do
    clear_schema
    create_texttbl
    medium_row = ["1", "a"*16*1024]

    expects(:schema).with().returns([{"tables" => [texttbl_def]}])
    expects(:rows).in_sequence.with("texttbl", [], []).returns([medium_row, []])
    expects(:quit)
    receive_commands

    assert_equal [medium_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles hashing long values" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (1, '#{'a'*80*1024}')"

    long_row = ["1", "a"*80*1024]

    expects(:schema).with().returns([{"tables" => [texttbl_def]}])
    expects(:hash).in_sequence.with("texttbl", [], ["1"]).returns([hash_and_count_of([long_row])])
    expects(:rows).in_sequence.with("texttbl", ["1"], []).returns([[]])
    expects(:quit)
    receive_commands

    assert_equal [long_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving long values" do
    clear_schema
    create_texttbl
    long_row = ["1", "a"*80*1024]

    expects(:schema).with().returns([{"tables" => [texttbl_def]}])
    expects(:rows).in_sequence.with("texttbl", [], []).returns([long_row, []])
    expects(:quit)
    receive_commands

    assert_equal [long_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end
end
