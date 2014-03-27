require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  test_each "accepts an empty list of tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => []
    expect_command Commands::QUIT
  end

  test_each "accepts a matching list of tables with matching schema" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::QUIT
  end


  test_each "complains about an empty list of tables on a non-empty database" do
    clear_schema
    create_footbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra table footbl") do
      send_command Commands::SCHEMA, "tables" => []
      unpacker.read rescue nil      
    end
  end

  test_each "complains about a non-empty list of tables on an empty database" do
    clear_schema

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about a missing table before other tables" do
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about a missing table between other tables" do
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table middletbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about a missing table after other tables" do
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about extra tables before other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra table footbl") do
      send_command Commands::SCHEMA, "tables" => [middletbl_def, secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about extra tables between other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra table middletbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about extra tables after other tables" do
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def, middletbl_def]
      unpacker.read rescue nil      
    end
  end


  test_each "doesn't complain about a missing table before other tables if told to ignore the table, and doesn't ask for its data" do
    program_args << 'footbl'
    clear_schema
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    unpacker.read rescue nil
  end

  test_each "doesn't complain about a missing table between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    unpacker.read rescue nil
  end

  test_each "doesn't complain about a missing table after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    unpacker.read rescue nil
  end

  test_each "doesn't complain about extra tables before other tables if told to ignore the table" do
    program_args << 'footbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [middletbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    unpacker.read rescue nil
  end

  test_each "doesn't complain about extra tables between other tables if told to ignore the table" do
    program_args << 'middletbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, secondtbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["secondtbl"]
    send_command   Commands::ROWS, [], []
    unpacker.read rescue nil
  end

  test_each "doesn't complain about extra tables after other tables if told to ignore the table" do
    program_args << 'secondtbl'
    clear_schema
    create_footbl
    create_middletbl
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [footbl_def, middletbl_def]
    expect_command Commands::OPEN, ["footbl"]
    send_command   Commands::ROWS, [], []
    expect_command Commands::OPEN, ["middletbl"]
    send_command   Commands::ROWS, [], []
    unpacker.read rescue nil
  end


  test_each "complains about missing columns before other columns" do
    clear_schema
    create_secondtbl
    execute("ALTER TABLE secondtbl DROP COLUMN pri1")
    execute("CREATE UNIQUE INDEX pri2_key ON secondtbl (pri2)") # needed for pg, which drops the primary key when a column is removed; mysql just removes the removed column from the index

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing column pri1 on table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about missing columns between other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN another_col")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing column another_col on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about missing columns after other columns" do
    clear_schema
    create_footbl
    execute("ALTER TABLE footbl DROP COLUMN col3")

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing column col3 on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about extra columns before other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra column col1 on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => footbl_def["columns"][1..-1])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about extra columns between other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra column another_col on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => footbl_def["columns"][0..0] + footbl_def["columns"][2..-1])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about extra columns after other columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra column col3 on table footbl") do
      send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => footbl_def["columns"][0..-2])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about misordered columns" do
    clear_schema
    create_footbl
    # postgresql doesn't support BEFORE/AFTER so we do this test by changing the expected schema instead

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Misordered column another_col on table footbl, should have col1 first") do
      send_command Commands::SCHEMA, "tables" => [footbl_def.merge("columns" => footbl_def["columns"][1..-1] + footbl_def["columns"][0..0])]
      unpacker.read rescue nil      
    end
  end


  test_each "complains about column types not matching" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col VARCHAR(11)", "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col TYPE VARCHAR(11)"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Column another_col on table footbl should be INT but was VARCHAR") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about column nullability not matching" do
    clear_schema
    create_footbl
    execute({"mysql" => "ALTER TABLE footbl MODIFY another_col SMALLINT NOT NULL", "postgresql" => "ALTER TABLE footbl ALTER COLUMN another_col SET NOT NULL"}[@database_server])

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Column another_col on table footbl should be nullable but was not nullable") do
      send_command Commands::SCHEMA, "tables" => [footbl_def]
      unpacker.read rescue nil      
    end
  end


  test_each "complains if the primary key column order doesn't match" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (pri1, pri2)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [0, 1])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains if there are extra primary key columns after the matching part" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (pri2, pri1, sec)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [1, 0, 2])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains if there are extra primary key columns before the matching part" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching primary key (pri2, pri1) on table secondtbl, should have (sec, pri2, pri1)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("primary_key_columns" => [2, 1, 0])]
      unpacker.read rescue nil      
    end
  end


  test_each "complains about extra keys" do
    clear_schema
    create_secondtbl
    execute "CREATE INDEX extrakey ON secondtbl (sec, tri)"

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Extra key extrakey on table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about missing keys" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Missing key missingkey on table secondtbl") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => secondtbl_def["keys"] + [secondtbl_def["keys"][0].merge("name" => "missingkey")])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about keys whose unique flag doesn't match" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching unique flag on table secondtbl key secidx") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => [secondtbl_def["keys"][0].merge("unique" => true)])]
      unpacker.read rescue nil      
    end
  end

  test_each "complains about about column list differences on keys" do
    clear_schema
    create_secondtbl

    expect_handshake_commands
    expect_command Commands::SCHEMA
    expect_stderr("Mismatching columns (sec) on table secondtbl key secidx, should have (tri, pri2)") do
      send_command Commands::SCHEMA, "tables" => [secondtbl_def.merge("keys" => [secondtbl_def["keys"][0].merge("columns" => [3, 1])])]
      unpacker.read rescue nil      
    end
  end
end
