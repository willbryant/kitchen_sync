require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class HashFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def setup_with_footbl(*handshake_args)
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str'), (100, 0, 'last')"
    @rows = [[2,    10,       "test"],
             [4,   nil,        "foo"],
             [5,   nil,          nil],
             [8,    -1, "longer str"],
             [100,   0,       "last"]]
    @keys = @rows.collect {|row| [row[0]]}
    send_handshake_commands(*handshake_args)
  end

  test_each "calculates the hash of all the rows whose key is greater than the first argument and not greater than the last argument, and if it matches, responds likewise with the hash of the next rows (doubling the count of rows hashed)" do
    setup_with_footbl
    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1])]
    expect_command Commands::HASH_NEXT, [@keys[1], @keys[3], hash_of(@rows[2..3])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2])]
    expect_command Commands::HASH_NEXT, [@keys[2], @keys[4], hash_of(@rows[3..4])]
  end

  test_each "starts from the first row if an empty array is given as the first argument" do
    setup_with_footbl

    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]
    expect_command Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2])]

    send_command   Commands::HASH_NEXT, [[], @keys[1], hash_of(@rows[0..1])]
    expect_command Commands::HASH_NEXT, [@keys[1], @keys[4], hash_of(@rows[2..4])]
  end

  test_each "sends back an empty rowset for the key range greater than the last row's key if the hash of the last row is given and matches" do
    setup_with_footbl

    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [@keys[-2], @keys[-1], hash_of(@rows[-1..-1])]
    expect_command Commands::ROWS, [@keys[-1], []]
  end

  test_each "sends back an empty rowset for the key range greater than the last row's key if the hash of the last set of rows is given and matches" do
    setup_with_footbl

    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [@keys[-4], @keys[-1], hash_of(@rows[-3..-1])]
    expect_command Commands::ROWS, [@keys[-1], []]
  end

  test_each "sends back its hash of half as many rows if the hash of multiple rows is given and it doesn't match, keeping track of the failed range end" do
    setup_with_footbl

    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2]).reverse]
    expect_command Commands::HASH_FAIL, [@keys[0], @keys[1], @keys[2], hash_of(@rows[1..1])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[4], hash_of(@rows[1..4]).reverse]
    expect_command Commands::HASH_FAIL, [@keys[0], @keys[2], @keys[4], hash_of(@rows[1..2])]
  end

  test_each "sends back the row instead if the hash of only one is given and it doesn't match" do
    setup_with_footbl

    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1]).reverse]
    expect_command Commands::ROWS_AND_HASH_NEXT, [@keys[0], @keys[1], @keys[2], hash_of(@rows[2..2])],
                   @rows[1]

    send_command   Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0]).reverse]
    expect_command Commands::ROWS_AND_HASH_NEXT, [[], @keys[0], @keys[1], hash_of(@rows[1..1])],
                   @rows[0]
  end

  test_each "sends multiple initial rows if their data size is approximately between half and the full target block size" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (1, '#{'x'*30*1024}'), (2, '#{'x'*30*1024}'), (3, '#{'x'*30*1024}')"

    @rows = [[1, "x"*30*1024],
             [2, "x"*30*1024],
             [3, "x"*30*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    send_handshake_commands(64*1024)

    send_command   Commands::OPEN, ["texttbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[1], hash_of(@rows[0..1])]
  end

  test_each "sends single initial rows if its data size is approximately between half and the full target block size" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (1, '#{'x'*60*1024}'), (2, '#{'x'*60*1024}'), (3, '#{'x'*60*1024}')"

    @rows = [[1, "x"*60*1024],
             [2, "x"*60*1024],
             [3, "x"*60*1024]]
    @keys = @rows.collect {|row| [row[0]]}

    send_handshake_commands(64*1024)

    send_command   Commands::OPEN, ["texttbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]
  end

  test_each "sends back the rows instead of the hash of multiple rows is given and it doesn't match, but the range has less than approximately the target block size of data" do
    clear_schema
    create_texttbl
    execute "INSERT INTO texttbl VALUES (1, '#{'x'*20*1024}'), (2, '#{'x'*20*1024}'), (3, '#{'x'*20*1024}'), (4, '#{'x'*20*1024}'), (5, '#{'x'*80*1024}'), (6, '#{'x'*80*1024}'), (7, '#{'x'*80*1024}')"

    @rows = [[1, "x"*20*1024],
             [2, "x"*20*1024],
             [3, "x"*20*1024],
             [4, "x"*20*1024],
             [5, "x"*80*1024],
             [6, "x"*80*1024],
             [7, "x"*80*1024]]
    @keys = @rows.collect {|row| [row[0]]}
    send_handshake_commands(64*1024)

    send_command   Commands::OPEN, ["texttbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[1], hash_of(@rows[0..1])]

    send_command   Commands::HASH_NEXT, [@keys[1], @keys[3], hash_of(@rows[2..3]).reverse]
    expect_command Commands::ROWS_AND_HASH_NEXT, [@keys[1], @keys[3], @keys[4], hash_of(@rows[4..4])],
                   @rows[2],
                   @rows[3]

    send_command   Commands::HASH_NEXT, [@keys[4], @keys[6], hash_of(@rows[5..6]).reverse]
    expect_command Commands::HASH_FAIL, [@keys[4], @keys[5], @keys[6], hash_of(@rows[5..5])]
  end

  test_each "supports composite keys" do
    clear_schema
    create_secondtbl
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1), (9, 968116383, 'aa', 9), (100, 100, 'aa', 100), (340, 363401169, 'ab', 20)"
    @rows = [[100,       100, "aa", 100], # first because the second column is the first term in the key so it's sorted like ["aa", 100]
             [  9, 968116383, "aa",   9],
             [340, 363401169, "ab",  20],
             [  2,   2349174, "xy",   1]]
    # note that the primary key columns are in reverse order to the table definition; the command arguments need to be given in the key order, but the column order for the results is unrelated
    @keys = @rows.collect {|row| [row[2], row[1]]}
    send_handshake_commands

    send_command   Commands::OPEN, ["secondtbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::HASH_NEXT, [      [], @keys[0], hash_of(@rows[0..0])]
    expect_command Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2])]

    send_command   Commands::HASH_NEXT, [["aa", "101"], @keys[1], hash_of(@rows[1..1])]
    expect_command Commands::HASH_NEXT, [@keys[1], @keys[3], hash_of(@rows[2..3])]

    send_command   Commands::HASH_NEXT, [      [], ["aa", "101"], hash_of(@rows[0..0])]
    expect_command Commands::HASH_NEXT, [["aa", "101"], @keys[2], hash_of(@rows[1..2])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1])]
    expect_command Commands::HASH_NEXT, [@keys[1], @keys[3], hash_of(@rows[2..3])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2])]
    expect_command Commands::HASH_NEXT, [@keys[2], @keys[3], hash_of(@rows[3..3])]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1]).reverse]
    expect_command Commands::ROWS_AND_HASH_NEXT, [@keys[0], @keys[1], @keys[2], hash_of(@rows[2..2])],
                   @rows[1]

    send_command   Commands::HASH_NEXT, [@keys[0], ["aa", "101"], hash_of(@rows[1..1])]
    expect_command Commands::ROWS_AND_HASH_NEXT, [@keys[0], @keys[1], @keys[2], hash_of(@rows[2..2])],
                   @rows[1]
  end

  test_each "optionally supports xxHash64 hashes" do
    setup_with_footbl(1, HashAlgorithm::XXH64)
    send_command   Commands::OPEN, ["footbl"]
    expect_command Commands::HASH_NEXT, [[], @keys[0], hash_of(@rows[0..0], HashAlgorithm::XXH64)]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[1], hash_of(@rows[1..1], HashAlgorithm::XXH64)]
    expect_command Commands::HASH_NEXT, [@keys[1], @keys[3], hash_of(@rows[2..3], HashAlgorithm::XXH64)]

    send_command   Commands::HASH_NEXT, [@keys[0], @keys[2], hash_of(@rows[1..2], HashAlgorithm::XXH64)]
    expect_command Commands::HASH_NEXT, [@keys[2], @keys[4], hash_of(@rows[3..4], HashAlgorithm::XXH64)]
  end
end
