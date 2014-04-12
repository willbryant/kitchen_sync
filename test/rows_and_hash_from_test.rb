require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class RowsAndHashFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def create_some_data
    create_some_tables
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (4, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"
    @rows = [["2",  "10",       "test"],
             ["4",   nil,        "foo"],
             ["5",   nil,          nil],
             ["8",  "-1", "longer str"]]
    @keys = @rows.collect {|row| [row[0]]}
  end

  test_each "returns the requested rows and if the given hash matches, gives the hash for the subsequent range of rows" do
    create_some_data
    send_handshake_commands

    send_command   Commands::OPEN, "footbl"
    expect_command Commands::HASH_NEXT,
                   [[], ["2"], hash_of(@rows[0..0])]

    send_command   Commands::ROWS_AND_HASH_NEXT, [], ["2"], ["4"], hash_of(@rows[1..1])
    expect_command Commands::ROWS,
                   [[], ["2"]],
                   @rows[0]
    expect_command Commands::HASH_NEXT, [["4"], ["8"], hash_of(@rows[2..3])] # row count doubled, since the last matched
  end

  test_each "sends the requested rows together with the subsequent rows if the given hash of multiple rows doesn't match" do
    create_some_data
    send_handshake_commands

    send_command   Commands::OPEN, "footbl"
    expect_command Commands::HASH_NEXT,
                   [[], ["2"], hash_of(@rows[0..0])]

    send_command   Commands::ROWS_AND_HASH_NEXT, [], ["2"], ["5"], hash_of(@rows[1..2]).reverse
    expect_command Commands::ROWS_AND_HASH_FAIL,
                   [[], ["2"], ["4"], ["5"], hash_of(@rows[1..1])], # row count halved, and failing range end key given
                   @rows[0]
  end

  test_each "sends the requested rows together with the subsequent rows if the given hash of only one row doesn't match" do
    create_some_data
    send_handshake_commands

    send_command   Commands::OPEN, "footbl"
    expect_command Commands::HASH_NEXT,
                   [[], ["2"], hash_of(@rows[0..0])]

    send_command   Commands::ROWS_AND_HASH_NEXT, [], ["2"], ["4"], hash_of(@rows[1..1]).reverse
    expect_command Commands::ROWS_AND_HASH_NEXT,
                   [[], ["4"], ["5"], hash_of(@rows[2..2])], # row count not doubled, since the last didn't match
                   @rows[0],
                   @rows[1]
  end
end
