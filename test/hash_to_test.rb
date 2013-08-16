require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class HashToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def setup
    # checking how protocol versions are handled is covered in protocol_versions_test; here we just need to get past that to get on to the commands we want to test
    expects(:protocol).with(CURRENT_PROTOCOL_VERSION).returns(CURRENT_PROTOCOL_VERSION)
  end

  test_each "is initially asked for a hash with no key range and a row count of 1, and terminates if we return nothing with an empty table" do
    clear_schema
    create_footbl

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).with("footbl", [], 1).returns([hash_of([]), []])
    expects(:quit)
    receive_commands
  end

  test_each "it moves onto the rows after the last key returned, and doubles the row count, if we return a matching hash" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test')"

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).in_sequence.with("footbl", [], 1).returns([hash_of([["2", "10", "test"]]), ["2"]])
    expects(:hash).in_sequence.with("footbl", ["2"], 2).returns([hash_of([]), []])
    expects(:quit)
    receive_commands
  end

  test_each "doubles the row count each iteration if we continue to return matching hashes" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo')"

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).in_sequence.with("footbl", [], 1).returns([hash_of([["2", "10", "test"]]), ["2"]])
    expects(:hash).in_sequence.with("footbl", ["2"], 2).returns([hash_of([["3", nil, "foo"]]), ["3"]])
    expects(:hash).in_sequence.with("footbl", ["3"], 4).returns([hash_of([]), []])
    expects(:quit)
    receive_commands
  end

  test_each "clears the table if we indicate there's no rows straight away" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).in_sequence.with("footbl", [], 1).returns([hash_of([]), []])
    expects(:quit)
    receive_commands

    assert_equal [],
                 query("SELECT * FROM footbl")
  end

  test_each "clears the remainder of the table if we indicate there's no rows after an initial match" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).in_sequence.with("footbl", [], 1).returns([hash_of([["2", "10", "test"]]), ["2"]])
    expects(:hash).in_sequence.with("footbl", ["2"], 2).returns([hash_of([]), []])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"]],
                 query("SELECT * FROM footbl")
  end

  test_each "clears the remainder of the table if we indicate there's no rows after multiple matches" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 10, 'test'), (3, NULL, 'foo'), (5, NULL, NULL), (8, -1, 'longer str')"

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).in_sequence.with("footbl", [], 1).returns([hash_of([["2", "10", "test"]]), ["2"]])
    expects(:hash).in_sequence.with("footbl", ["2"], 2).returns([hash_of([["3", nil, "foo"]]), ["3"]])
    expects(:hash).in_sequence.with("footbl", ["3"], 4).returns([hash_of([]), []])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"], ["3", nil, "foo"]],
                 query("SELECT * FROM footbl")
  end

  test_each "is asked for the row if we return a different hash straight away, after which it carries on from the last key" do
    clear_schema
    create_footbl
    execute "INSERT INTO footbl VALUES (2, 1, 'different')"

    expects(:schema).with().returns({"tables" => [footbl_def]})
    expects(:hash).in_sequence.with("footbl", [], 1).returns([hash_of([["2", "10", "test"]]), ["2"]])
    rows = expects(:rows).in_sequence.with("footbl", [], ["2"]) do |*args|
      # the rows command returns multiple results rather than one array of arrays, to avoid the need to know the number of rows in advance
      spawner.send_result ["2", "10", "test"]
      rows.returns([])
    end
    expects(:hash).in_sequence.with("footbl", ["2"], 1).returns([hash_of([]), []])
    expects(:quit)
    receive_commands

    assert_equal [["2", "10", "test"]],
                 query("SELECT * FROM footbl")
  end
end
