require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SyncToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def setup_with_footbl
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str'), (101, 0, NULL), (1000, 0, NULL), (1001, 0, 'last')"
    @rows = [[2,     10,       "test"],
             [4,    nil,        "foo"],
             [5,    nil,          nil],
             [8,     -1, "longer str"],
             [101,    0,          nil],
             [1000,   0,          nil],
             [1001,   0,       "last"]]
    @keys = @rows.collect {|row| [row[0]]}
  end

  test_each "is immediately sent all rows if the other end has an empty table, and finishes without needing to make any changes if the table is empty" do
    clear_schema
    create_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::QUIT

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "is immediately sent all rows if the other end has an empty table, and clears the table if it is not empty" do
    clear_schema
    setup_with_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::QUIT

    assert_equal [],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "accepts matching hashes and asked for the hash of the next row(s), doubling the number of rows" do
    setup_with_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2])]
    send_command   Commands::HASH_NEXT, @keys[2], @keys[6], hash_of(@rows[3..6])
    expect_command Commands::ROWS, [@keys[-1], []]
    send_command   Commands::ROWS, @keys[-1], []
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "requests and applies the row if we send a different hash for a single row, and gives the hash after that" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 2"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::ROWS_AND_HASH_NEXT, [[], @keys[0], @keys[1], hash_of(@rows[1..1])]
    send_results   Commands::ROWS, # we could combo this and do a rows_and_hash back, but that wouldn't always be possible - we might need a rows PLUS a rows_and_hash (if they next hash they'd given didn't match), and we might need a rows plus a gap plus a hash, so we haven't implemented that
                   [[], @keys[0]],
                   @rows[0]
    send_command   Commands::HASH_NEXT, @keys[1], @keys[3], hash_of(@rows[2..3])
    expect_command Commands::HASH_NEXT, [@keys[3], @keys[-1], hash_of(@rows[4..-1])]
    send_command   Commands::ROWS, @keys[-1], []
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "reduces the search range and tries again if we send a different hash for multiple rows" do
    setup_with_footbl
    execute "UPDATE footbl SET col3 = 'different' WHERE col1 = 101"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2])]
    send_command   Commands::HASH_NEXT, @keys[2], @keys[6], hash_of(@rows[3..6])
    expect_command Commands::HASH_FAIL, [@keys[2], @keys[4], @keys[6], hash_of([@rows[3], [101, 0, "different"]])]
    send_command   Commands::HASH_FAIL, @keys[2], @keys[3], @keys[4], hash_of(@rows[3..3])
    expect_command Commands::ROWS_AND_HASH_NEXT, [@keys[3], @keys[4], @keys[5], hash_of(@rows[5..5])] # note that the other end has deduced that rows[4] is the problem, so it is requesting that directly rather than giving its hash
    send_results   Commands::ROWS,
                   [@keys[3], @keys[4]],
                   @rows[4]
    send_command   Commands::HASH_NEXT, @keys[4], @keys[5], hash_of(@rows[5..5])
    expect_command Commands::HASH_NEXT, [@keys[5], @keys[6], hash_of(@rows[6..6])]
    send_command   Commands::ROWS, @keys[-1], []
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles data after nil elements" do
    clear_schema
    create_footbl
    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_results   Commands::ROWS,
                   [[], []],
                   [2, nil, nil],
                   [3, nil,  "foo"]
    expect_command Commands::QUIT

    assert_equal [[2, nil,   nil],
                  [3, nil, "foo"]],
                 query("SELECT * FROM footbl ORDER BY col1")
  end

  test_each "handles hashing medium values" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (0, 'test'), (1, '#{'a'*16*1024}')"

    @rows = [[0, "test"],
             [1, "a"*16*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [texttbl_def]
    expect_command Commands::OPEN, ["texttbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1])]
    send_command   Commands::ROWS, @keys[1], []
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving medium values" do
    clear_schema
    create_texttbl

    @rows = [[1, "a"*16*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [texttbl_def]
    expect_command Commands::OPEN, ["texttbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::ROWS, [[], []]
    send_results   Commands::ROWS,
                   [[], []],
                   @rows[0]
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles hashing long values" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (0, 'test'), (1, '#{'a'*80*1024}')"

    @rows = [[0, "test"],
             [1, "a"*80*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [texttbl_def]
    expect_command Commands::OPEN, ["texttbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1])]
    send_command   Commands::ROWS, @keys[1], []
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving long values" do
    clear_schema
    create_texttbl

    @rows = [[1, "a"*80*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [texttbl_def]
    expect_command Commands::OPEN, ["texttbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::ROWS, [[], []]
    send_results   Commands::ROWS,
                   [[], []],
                   @rows[0]
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM texttbl ORDER BY pri")
  end

  test_each "handles requesting and saving arbitrary binary values in BLOB fields" do
    clear_schema
    create_texttbl

    bytes = (0..255).to_a.pack("C*")
    bytes = bytes.reverse + bytes
    row = [1] + [nil]*(misctbl_def["columns"].size - 2) + [bytes]
    @rows = [row]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [misctbl_def]
    expect_command Commands::OPEN, ["misctbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::ROWS, [[], []]
    send_results   Commands::ROWS,
                   [[], []],
                   @rows[0]
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM misctbl ORDER BY pri")
  end

  test_each "handles reusing unique values that were previously on later rows" do
    setup_with_footbl
    execute "CREATE UNIQUE INDEX unique_key ON footbl (col3)"

    @orig_rows = @rows.collect {|row| row.dup}
    @rows[0][-1] = @rows[-1][-1] # reuse this value from the last row
    @rows[-1][-1] = "new value"  # and change it there to something else

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def.merge("keys" => [{"name" => "unique_key", "unique" => true, "columns" => [2]}])]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::ROWS_AND_HASH_NEXT, [[], @keys[0], @keys[1], hash_of(@orig_rows[1..1])]
    send_results   Commands::ROWS,
                   [[], @keys[0]],
                   @rows[0]
    send_command   Commands::HASH_NEXT, @keys[1], @keys[3], hash_of(@rows[2..3])
    expect_command Commands::HASH_NEXT, [@keys[3], @keys[6], hash_of(@orig_rows[4..6])]
    send_command   Commands::HASH_NEXT, @keys[3], @keys[4], hash_of(@rows[4..4])
    expect_command Commands::HASH_NEXT, [@keys[4], @keys[6], hash_of(@orig_rows[5..6])]
    send_results   Commands::ROWS,
                   [@keys[6], []],
                   @rows[6]
    expect_command Commands::QUIT

    assert_equal @rows,
                 query("SELECT * FROM footbl ORDER BY col1")
  end
end
