require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "returns an empty list of tables on an empty database" do
    clear_schema

    send_handshake_commands
    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => []}]
  end

  test_each "returns the name of the tables and the names of their columns" do
    clear_schema
    create_footbl
    create_secondtbl
    create_texttbl
    create_misctbl
    send_handshake_commands
    
    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [footbl_def, misctbl_def, secondtbl_def, texttbl_def]}]
  end

  test_each "selects the first unique key with no nullable columns if there is no primary key" do # rejecting the case where there is no such table is handled at the 'to' end
    clear_schema
    create_noprimarytbl
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimarytbl_def]}]
  end

  test_each "selects no key if there is no primary key and no unique key with non-nullable columns" do
    clear_schema
    create_noprimaryjointbl(create_keys: false)
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimaryjointbl_def(create_keys: false)]}]
  end

  test_each "selects a key that covers all the non-nullable columns as a partial key if there is no primary key and no unique key but there are only non-nullable columns" do
    clear_schema
    create_noprimaryjointbl(create_keys: true)
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimaryjointbl_def(create_keys: true)]}]
  end

  test_each "selects a key that covers all the non-nullable columns as a partial key if there is no primary key and no suitable unique key but all the non-nullable columns are included in the key" do
    clear_schema
    create_noprimaryjointbl(create_keys: true)
    execute "ALTER TABLE noprimaryjointbl ADD COLUMN dummy INT"
    send_handshake_commands

    table = noprimaryjointbl_def(create_keys: true)
    table["columns"] = table["columns"] + [{"name" => "dummy", "column_type" => ColumnTypes::SINT, "size" => 4}]
    table["secondary_sort_columns"] = [2]

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [table]}]
  end

  test_each "selects a key as a partial key if there is no primary key and no suitable unique key and this key is the highest-cardinality" do
    clear_schema
    create_noprimaryjointbl(create_keys: false)
    execute "ALTER TABLE noprimaryjointbl ADD COLUMN dummy INT NOT NULL DEFAULT 0"
    execute "CREATE INDEX index_by_table1_id ON noprimaryjointbl (table1_id)"
    execute "CREATE INDEX index_by_table2_id ON noprimaryjointbl (table2_id)" # you'd never create this one and the next one in a real table since it would be redundant
    execute "CREATE INDEX index_by_table2_id_and_table1_id ON noprimaryjointbl (table2_id, table1_id)"
    execute "INSERT INTO noprimaryjointbl (table1_id, table2_id) VALUES (1, 10), (2, 10), (2, 11), (2, 12), (2, 13)" # so (table2, table1) has cardinality 5 whereas (table1) only has cardinality 2 and (table2) cardinality 4
    connection.analyze_table "noprimaryjointbl"
    send_handshake_commands

    table = noprimaryjointbl_def(create_keys: false)
    table["columns"] = table["columns"] + [{"name" => "dummy", "column_type" => ColumnTypes::SINT, "size" => 4, "nullable" => false, "default_value" => "0"}]
    table["primary_key_type"] = PrimaryKeyType::PARTIAL_KEY
    table["primary_key_columns"] = [1, 0]
    table["secondary_sort_columns"] = [2]
    table["keys"] = [
      {"name" => "index_by_table1_id",               "unique" => false, "columns" => [0]},
      {"name" => "index_by_table2_id",               "unique" => false, "columns" => [1]},
      {"name" => "index_by_table2_id_and_table1_id", "unique" => false, "columns" => [1, 0]}
    ]

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [table]}]
  end

  test_each "shows the default values for columns" do
    clear_schema
    create_defaultstbl
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [defaultstbl_def]}]
  end

  test_each "describes identity/serial/auto_increment sequence columns" do
    clear_schema
    create_autotbl
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [autotbl_def]}]
  end

  test_each "returns the appropriate representation of adapter-specific column definitions" do
    clear_schema
    create_adapterspecifictbl

    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [adapterspecifictbl_def]}]
  end

  test_each "ignores views" do
    clear_schema
    create_footbl
    create_view

    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [footbl_def]}]

    send_command   Commands::ROWS, [footbl_def["name"], [], []]
    expect_command Commands::ROWS,
                   [footbl_def["name"], [], []]
  end

  test_each "reports unsupported column types" do
    clear_schema
    create_unsupportedtbl

    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [unsupportedtbl_def]}]
  end
end
