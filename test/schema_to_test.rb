require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def setup
    expect_handshake_commands
  end

  test_each "accepts an empty list of tables on an empty database" do
    clear_schema

    expects(:schema).with().returns([{"tables" => []}])
    expects(:quit)
    receive_commands
  end

  test_each "accepts a matching list of tables with matching schema" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    expects(:rows).times(3).returns(
      [[Commands::ROWS, "footbl", [], []], []],
      [[Commands::ROWS, "middletbl", [], []], []],
      [[Commands::ROWS, "secondtbl", [], []], []])
    expects(:quit)
    receive_commands
  end


  test_each "complains about an empty list of tables on a non-empty database" do
    clear_schema
    create_footbl

    expects(:schema).with().returns([{"tables" => []}])
    stubs(:quit)
    expect_stderr("Extra table footbl") do
      receive_commands
    end
  end

  test_each "complains about a non-empty list of tables on an empty database" do
    clear_schema

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    stubs(:quit)
    expect_stderr("Missing table footbl") do
      receive_commands
    end
  end

  test_each "complains about a missing table before other tables" do
    clear_schema
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Missing table footbl") do
      receive_commands
    end
  end

  test_each "complains about a missing table between other tables" do
    clear_schema
    create_footbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Missing table middletbl") do
      receive_commands
    end
  end

  test_each "complains about a missing table after other tables" do
    clear_schema
    create_footbl
    create_middletbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Missing table secondtbl") do
      receive_commands
    end
  end

  test_each "complains about extra tables before other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [middletbl_def, secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Extra table footbl") do
      receive_commands
    end
  end

  test_each "complains about extra tables between other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Extra table middletbl") do
      receive_commands
    end
  end

  test_each "complains about extra tables after other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def]}])
    stubs(:quit)
    expect_stderr("Extra table secondtbl") do
      receive_commands
    end
  end


  test_each "doesn't complain about a missing table before other tables if told to ignore the table, and doesn't ask for its data" do
    program_args << 'footbl'
    clear_schema
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    expects(:rows).times(2).returns(
      [[Commands::ROWS, "middletbl", [], []], []],
      [[Commands::ROWS, "secondtbl", [], []], []])
    expects(:quit)
    receive_commands
  end

  test_each "doesn't complain about a missing table between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    expects(:rows).times(2).returns(
      [[Commands::ROWS, "footbl", [], []], []],
      [[Commands::ROWS, "secondtbl", [], []], []])
    expects(:quit)
    receive_commands
  end

  test_each "doesn't complain about a missing table after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def, secondtbl_def]}])
    expects(:rows).times(2).returns(
      [[Commands::ROWS, "footbl", [], []], []],
      [[Commands::ROWS, "middletbl", [], []], []])
    expects(:quit)
    receive_commands
  end

  test_each "doesn't complain about extra tables before other tables if told to ignore the table" do
    program_args << 'footbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [middletbl_def, secondtbl_def]}])
    expects(:rows).times(2).returns(
      [[Commands::ROWS, "middletbl", [], []], []],
      [[Commands::ROWS, "secondtbl", [], []], []])
    expects(:quit)
    receive_commands
  end

  test_each "doesn't complain about extra tables between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, secondtbl_def]}])
    expects(:rows).times(2).returns(
      [[Commands::ROWS, "footbl", [], []], []],
      [[Commands::ROWS, "secondtbl", [], []], []])
    expects(:quit)
    receive_commands
  end

  test_each "doesn't complain about extra tables after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [footbl_def, middletbl_def]}])
    expects(:rows).times(2).returns(
      [[Commands::ROWS, "footbl", [], []], []],
      [[Commands::ROWS, "middletbl", [], []], []])
    expects(:quit)
    receive_commands
  end


  test_each "complains about missing columns before other columns" do
    clear_schema
    create_secondtbl
    execute("ALTER TABLE secondtbl DROP COLUMN pri1")
    execute("CREATE UNIQUE INDEX pri2_key ON secondtbl (pri2)") # needed for pg, which drops the primary key when a column is removed; mysql just removes the removed column from the index

    expects(:schema).with().returns([{"tables" => [secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Missing column pri1 on table secondtbl") do
      receive_commands
    end
  end

  test_each "complains about missing columns between other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN another_col")

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    stubs(:quit)
    expect_stderr("Missing column another_col on table footbl") do
      receive_commands
    end
  end

  test_each "complains about missing columns after other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN col3")

    expects(:schema).with().returns([{"tables" => [footbl_def]}])
    stubs(:quit)
    expect_stderr("Missing column col3 on table footbl") do
      receive_commands
    end
  end

  test_each "complains about extra columns before other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expects(:schema).with().returns([{"tables" => [footbl_def.merge("columns" => footbl_def["columns"][1..-1])]}])
    stubs(:quit)
    expect_stderr("Extra column col1 on table footbl") do
      receive_commands
    end
  end

  test_each "complains about extra columns between other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expects(:schema).with().returns([{"tables" => [footbl_def.merge("columns" => footbl_def["columns"][0..0] + footbl_def["columns"][2..-1])]}])
    stubs(:quit)
    expect_stderr("Extra column another_col on table footbl") do
      receive_commands
    end
  end

  test_each "complains about extra columns after other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expects(:schema).with().returns([{"tables" => [footbl_def.merge("columns" => footbl_def["columns"][0..-2])]}])
    stubs(:quit)
    expect_stderr("Extra column col3 on table footbl") do
      receive_commands
    end
  end

  test_each "complains about misordered columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expects(:schema).with().returns([{"tables" => [footbl_def.merge("columns" => footbl_def["columns"][1..-1] + footbl_def["columns"][0..0])]}])
    stubs(:quit)
    expect_stderr("Misordered column another_col on table footbl, should have col1 first") do
      receive_commands
    end
  end


  test_each "complains if the primary key column order doesn't match" do
    clear_schema
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [secondtbl_def.merge("primary_key_columns" => [0, 1])]}])
    stubs(:quit)
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (pri1, pri2)") do
      receive_commands
    end
  end

  test_each "complains if there are extra primary key columns after the matching part" do
    clear_schema
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [secondtbl_def.merge("primary_key_columns" => [1, 0, 2])]}])
    stubs(:quit)
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (pri2, pri1, sec)") do
      receive_commands
    end
  end

  test_each "complains if there are extra primary key columns before the matching part" do
    clear_schema
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [secondtbl_def.merge("primary_key_columns" => [2, 1, 0])]}])
    stubs(:quit)
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (sec, pri2, pri1)") do
      receive_commands
    end
  end


  test_each "complains about extra keys" do
    clear_schema
    create_secondtbl
    execute "CREATE INDEX extrakey ON secondtbl (sec, tri)"

    expects(:schema).with().returns([{"tables" => [secondtbl_def]}])
    stubs(:quit)
    expect_stderr("Extra key extrakey on table secondtbl") do
      receive_commands
    end
  end

  test_each "complains about missing keys" do
    clear_schema
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [secondtbl_def.merge("keys" => secondtbl_def["keys"] + [secondtbl_def["keys"][0].merge("name" => "missingkey")])]}])
    stubs(:quit)
    expect_stderr("Missing key missingkey on table secondtbl") do
      receive_commands
    end
  end

  test_each "complains about keys whose unique flag doesn't match" do
    clear_schema
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [secondtbl_def.merge("keys" => [secondtbl_def["keys"][0].merge("unique" => true)])]}])
    stubs(:quit)
    expect_stderr("Mismatching unique flag on table secondtbl key secidx") do
      receive_commands
    end
  end

  test_each "complains about about column list differences on keys" do
    clear_schema
    create_secondtbl

    expects(:schema).with().returns([{"tables" => [secondtbl_def.merge("keys" => [secondtbl_def["keys"][0].merge("columns" => [3, 1])])]}])
    stubs(:quit)
    expect_stderr("Mismatching columns (sec) on table secondtbl key secidx, should have (tri, pri2)") do
      receive_commands
    end
  end
end
