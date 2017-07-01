require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class RangeFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "returns empty arrays if the table is empty" do
    create_some_tables
    send_handshake_commands

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [], []]
  end

  test_each "returns the same key value twice if the table has exactly one row" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (42, 10, 'test')"
    send_handshake_commands

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [42], [42]]
  end

  test_each "returns the two keys in ascending order if the table has exactly two rows" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (42, 10, 'test')"
    execute "INSERT INTO footbl VALUES (23, 15, 'test2')"
    send_handshake_commands

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [23], [42]]
  end

  test_each "returns the lowest and highest keys if the table has more than two rows" do
    create_some_tables
    execute "INSERT INTO footbl VALUES (42, 10, 'test'), (23, 15, 'test2')"
    execute "INSERT INTO footbl VALUES (5, NULL, NULL), (4, NULL, 'foo'), (8, -1, 'longer str')"
    send_handshake_commands

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [4], [42]]
  end

  test_each "supports composite keys" do
    create_some_tables

    # note when reading these that the primary key columns are in reverse order to the table definition; the results need to be given in the key order, not the column order for the row as a whole
    execute "INSERT INTO secondtbl VALUES (2, 2349174, 'xy', 1), (9, 968116383, 'aa', 9), (100, 100, 'aa', 100), (340, 363401169, 'ab', 20)"
    send_handshake_commands
    send_command   Commands::RANGE, ["secondtbl"]
    expect_command Commands::RANGE,
                   ["secondtbl", ["aa", 100], ["xy", 2349174]]
  end

  test_each "sorts on each component of composite keys" do
    create_some_tables

    # note when reading these that the primary key columns are in reverse order to the table definition; the results need to be given in the key order, not the column order for the row as a whole
    execute "INSERT INTO secondtbl VALUES (9, 968116383, 'aa', 9), (100, 100, 'aa', 100)"
    send_handshake_commands

    send_command   Commands::RANGE, ["secondtbl"]
    expect_command Commands::RANGE,
                   ["secondtbl", ["aa", 100], ["aa", 968116383]]
  end

  test_each "supports reserved-word column names" do
    clear_schema
    create_reservedtbl
    send_handshake_commands

    send_command   Commands::RANGE, ["reservedtbl"]
    expect_command Commands::RANGE,
                   ["reservedtbl", [], []]
  end
end
