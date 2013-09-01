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

  def setup_with_footbl
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str'), (101, 0, NULL), (1000, 0, NULL), (1001, 0, 'last')"
    @rows = [["2",    "10",       "test"],
             ["4",     nil,        "foo"],
             ["5",     nil,          nil],
             ["8",    "-1", "longer str"],
             ["101",   "0",          nil],
             ["1000",  "0",          nil],
             ["1001",  "0",       "last"]]
    @keys = @rows.collect {|row| [row[0]]}
  end

  test_each "is immediately asked for all rows if the other end has an empty table, and no change is made" do
    clear_schema
    create_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:rows).with("footbl", [], []).
      returns([["rows", "footbl", [], []], []])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "is given the hash of the first row, accepts back the hash of the next row(s), and asks for the remaining rows if it has no more, and no change is made" do
    setup_with_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:hash).with("footbl", [], @keys[0], hash_of(@rows[0..0])).
      returns([["hash", "footbl", @keys[0], @keys[-1], hash_of(@rows[1..-1])]])
    expects(:rows).with("footbl", @keys[-1], []).
      returns([["rows", "footbl", @keys[-1], []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "requests hashes for twice as many each iteration while we continue to return matching hashes" do
    setup_with_footbl

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).with("footbl", [], @keys[0], hash_of(@rows[0..0])).
      returns([["hash", "footbl", @keys[0], @keys[2], hash_of(@rows[1..2])]])
    expects(:hash).with("footbl", @keys[2], @keys[6], hash_of(@rows[3..6])).
      returns([["rows", "footbl", @keys[6], []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "updates the table if we return a different row straight away, and then carries on with the next row after that" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 2"

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    expects(:hash).with("footbl", [], @keys[0], hash_of([["2", "10", "different"]])).
      returns([["rows", "footbl", [], @keys[0]], @rows[0], []])
    expects(:hash).with("footbl", @keys[0], @keys[1], hash_of(@rows[1..1])).
      returns([["hash", "footbl", @keys[1], @keys[-1], hash_of(@rows[2..-1])]])
    expects(:rows).with("footbl", @keys[-1], []).
      returns([["rows", "footbl", @keys[-1], []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "reduces the search range and tries again if we return a different hash for multiple rows" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 101"

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:hash).with("footbl", [], @keys[0], hash_of(@rows[0..0])).
      returns([["hash", "footbl", @keys[0], @keys[2], hash_of(@rows[1..2])]])
    expects(:hash).with("footbl", @keys[2], @keys[6], hash_of([@rows[3], ["101", "0", "different"], @rows[5], @rows[6]])).
      returns([["hash", "footbl", @keys[2], @keys[6], hash_of(@rows[3..6])]]) # real app would reduce the range returned by this side too
    expects(:hash).with("footbl", @keys[2], @keys[4], hash_of([@rows[3], ["101", "0", "different"]])).
      returns([["hash", "footbl", @keys[2], @keys[4], hash_of(@rows[3..4])]]) # same
    expects(:hash).with("footbl", @keys[2], @keys[3], hash_of(@rows[3..3])).
      returns([["hash", "footbl", @keys[3], @keys[5], hash_of(@rows[4..5])]]) # (not ideal, since really we know by now @rows[4] is the issue)
    expects(:hash).with("footbl", @keys[3], @keys[4], hash_of([["101", "0", "different"]])).
      returns([["rows", "footbl", @keys[3], @keys[4]], @rows[4], []])
    expects(:hash).with("footbl", @keys[4], @keys[5], hash_of(@rows[5..5])).
      returns([["hash", "footbl", @keys[5], @keys[6], hash_of(@rows[6..6])]])
    expects(:rows).with("footbl", @keys[-1], []).
      returns([["rows", "footbl", @keys[-1], []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles data after nil elements" do
    clear_schema
    create_footbl
    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:rows).in_sequence.with("footbl", [], []).
      returns([["rows", "footbl", [], []], ["2", nil, nil], ["3",  nil,  "foo"], []])
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

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:hash).in_sequence.with("texttbl", [], ["1"], hash_of([medium_row])).
      returns([["rows", "texttbl", ["1"], []], []])
    expects(:quit)
    receive_commands

    assert_equal [medium_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving medium values" do
    clear_schema
    create_texttbl
    medium_row = ["1", "a"*16*1024]

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:rows).in_sequence.with("texttbl", [], []).
      returns([["rows", "texttbl", [], []], medium_row, []])
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

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:hash).in_sequence.with("texttbl", [], ["1"], hash_of([long_row])).
      returns([["rows", "texttbl", ["1"], []], []])
    expects(:quit)
    receive_commands

    assert_equal [long_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving long values" do
    clear_schema
    create_texttbl
    long_row = ["1", "a"*80*1024]

    expects(:schema).with().
      returns([{"tables" => [texttbl_def]}])
    expects(:rows).in_sequence.with("texttbl", [], []).
      returns([["rows", "texttbl", [], []], long_row, []])
    expects(:quit)
    receive_commands

    assert_equal [long_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end
end
