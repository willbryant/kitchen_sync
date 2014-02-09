require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SyncToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def setup
    expect_handshake_commands
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
    expects(:open).with("footbl").
      returns([nil])
    expects(:rows).with([], []).
      returns([[Commands::ROWS, [], []], []])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "is given the hash of the first row, accepts back the hash of the next row(s), and asks for the remaining rows if it has no more, and no change is made" do
    setup_with_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([nil])
    expects(:hash).with([], @keys[0], hash_of(@rows[0..0])).
      returns([[Commands::HASH, @keys[0], @keys[-1], hash_of(@rows[1..-1])]])
    expects(:rows).with(@keys[-1], []).
      returns([[Commands::ROWS, @keys[-1], []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "requests hashes for twice as many each iteration while we continue to return matching hashes" do
    setup_with_footbl

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([nil])
    expects(:hash).with([], @keys[0], hash_of(@rows[0..0])).
      returns([[Commands::HASH, @keys[0], @keys[2], hash_of(@rows[1..2])]])
    expects(:hash).with(@keys[2], @keys[6], hash_of(@rows[3..6])).
      returns([[Commands::ROWS, @keys[6], []], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "updates the table if we return a different row straight away, and then carries on with the next row after that" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 2"

    expects(:schema).with().
      returns([{"tables" => [footbl_def]}])
    expects(:open).with("footbl").
      returns([nil])
    expects(:hash).with([], @keys[0], hash_of([["2", "10", "different"]])).
      returns([[Commands::ROWS, [], @keys[0]], @rows[0], [],
               [Commands::HASH, @keys[0], @keys[1], hash_of(@rows[1..1])]])
    expects(:hash).with(@keys[1], @keys[3], hash_of(@rows[2..3])).
      returns([[Commands::HASH, @keys[3], @keys[-1], hash_of(@rows[4..-1])]])
    expects(:rows).with(@keys[-1], []).
      returns([[Commands::ROWS, @keys[-1], []], []])
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
    expects(:open).with("footbl").
      returns([nil])
    expects(:hash).with([], @keys[0], hash_of(@rows[0..0])).
      returns([[Commands::HASH, @keys[0], @keys[2], hash_of(@rows[1..2])]])
    expects(:hash).with(@keys[2], @keys[6], hash_of([@rows[3], ["101", "0", "different"], @rows[5], @rows[6]])).
      returns([[Commands::HASH, @keys[2], @keys[6], hash_of(@rows[3..6])]]) # real app would reduce the range returned by this side too
    expects(:hash).with(@keys[2], @keys[4], hash_of([@rows[3], ["101", "0", "different"]])).
      returns([[Commands::HASH, @keys[2], @keys[4], hash_of(@rows[3..4])]]) # same
    expects(:hash).with(@keys[2], @keys[3], hash_of(@rows[3..3])).
      returns([[Commands::HASH, @keys[3], @keys[5], hash_of(@rows[4..5])]]) # (not ideal, since really we know by now @rows[4] is the issue)
    expects(:hash).with(@keys[3], @keys[4], hash_of([["101", "0", "different"]])).
      returns([[Commands::ROWS, @keys[3], @keys[4]], @rows[4], [],
               [Commands::HASH, @keys[4], @keys[5], hash_of(@rows[5..5])]])
    expects(:hash).with(@keys[5], @keys[6], hash_of(@rows[6..6])).
      returns([[Commands::ROWS, @keys[-1], []], []])
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
    expects(:open).with("footbl").
      returns([nil])
    expects(:rows).in_sequence.with([], []).
      returns([[Commands::ROWS, [], []], ["2", nil, nil], ["3",  nil,  "foo"], []])
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
    expects(:open).with("texttbl").
      returns([nil])
    expects(:hash).in_sequence.with([], ["1"], hash_of([medium_row])).
      returns([[Commands::ROWS, ["1"], []], []])
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
    expects(:open).with("texttbl").
      returns([nil])
    expects(:rows).in_sequence.with([], []).
      returns([[Commands::ROWS, [], []], medium_row, []])
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
    expects(:open).with("texttbl").
      returns([nil])
    expects(:hash).in_sequence.with([], ["1"], hash_of([long_row])).
      returns([[Commands::ROWS, ["1"], []], []])
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
    expects(:open).with("texttbl").
      returns([nil])
    expects(:rows).in_sequence.with([], []).
      returns([[Commands::ROWS, [], []], long_row, []])
    expects(:quit)
    receive_commands

    assert_equal [long_row],
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles reusing unique values that were previously on later rows" do
    setup_with_footbl
    execute "CREATE UNIQUE INDEX unique_key ON footbl (col3)"

    @orig_rows = @rows.collect {|row| row.dup}
    @rows[0][-1] = @rows[-1][-1] # reuse this value from the last row
    @rows[-1][-1] = "new value"  # and change it there to something else

    expects(:schema).with().
      returns([{"tables" => [footbl_def.merge("keys" => [{"name" => "unique_key", "unique" => true, "columns" => [2]}])]}])
    expects(:open).with("footbl").
      returns([nil])
    expects(:hash).with([], @keys[0], hash_of([["2", "10", "test"]])).
      returns([[Commands::ROWS, [], @keys[0]], @rows[0], [],
               [Commands::HASH, @keys[0], @keys[1], hash_of(@rows[1..1])]])
    expects(:hash).with(@keys[1], @keys[3], hash_of(@rows[2..3])).
      returns([[Commands::HASH, @keys[3], @keys[6], hash_of(@rows[4..6])]])
    expects(:hash).with(@keys[3], @keys[4], hash_of(@orig_rows[4..4])).
      returns([[Commands::HASH, @keys[4], @keys[6], hash_of(@orig_rows[5..6])]])
    expects(:rows).with(@keys[6], []).
      returns([[Commands::ROWS, @keys[6], []], @rows[6], []])
    expects(:quit)
    receive_commands

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end
end
