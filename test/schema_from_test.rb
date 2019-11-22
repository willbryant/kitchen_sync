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
    create_noprimarytbl(create_suitable_keys: false)
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimarytbl_def(create_suitable_keys: false)]}]
  end

  test_each "selects the first unique key with no nullable columns and no replace expression if there is no primary key" do
    clear_schema
    create_noprimarytbl
    send_handshake_commands(
      filters: {"noprimarytbl" => {"filter_expressions" => {"version" => "1"}}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimarytbl_def.merge("primary_key_columns" => [3])]}] # note it has looked on and found another suitable key
  end

  test_each "selects no key if there is no primary key and no unique key with non-nullable columns and no replace expression" do
    clear_schema
    create_noprimarytbl
    send_handshake_commands(
      filters: {"noprimarytbl" => {"filter_expressions" => {"version" => "1", "non_nullable" => "1"}}})

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimarytbl_def.merge("primary_key_columns" => [], "primary_key_type" => PrimaryKeyType::NO_AVAILABLE_KEY)]}]
  end

  test_each "selects a covering key if there is no primary key and no unique key but there are only non-nullable columns" do
    clear_schema
    create_noprimaryjointbl(create_keys: true)
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimaryjointbl_def(create_keys: true)]}]
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

  test_each "describes auto-generated columns" do
    omit "Database doesn't support auto-generated columns" unless connection.supports_generated_columns?
    clear_schema
    create_generatedtbl
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [generatedtbl_def]}]
  end

  ENDPOINT_ADAPTERS.each do |to_database_server, adapter_class|
    test_each "returns the appropriate representation of adapter-specific column definitions for use by #{to_database_server}" do
      that_adapter = adapter_class.new
      clear_schema
      create_adapterspecifictbl

      send_handshake_commands(accepted_types: that_adapter.supported_column_types)

      send_command   Commands::SCHEMA
      expect_command Commands::SCHEMA,
                     [{"tables" => [adapterspecifictbl_def(compatible_with: that_adapter)]}]
    end

    test_each "reports database-specific column types, using the 'unknown' type if #{to_database_server} is not the same database server" do
      that_adapter = adapter_class.new
      clear_schema
      create_unsupportedtbl

      send_handshake_commands(accepted_types: that_adapter.supported_column_types)

      table_def = unsupportedtbl_def
      table_def["columns"][-1]["column_type"] = ColumnType::UNKNOWN if to_database_server != @database_server

      send_command   Commands::SCHEMA
      expect_command Commands::SCHEMA,
                     [{"tables" => [table_def]}]
    end
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

  test_each "returns schema in legacy format for protocol version 7 and earlier, without an explicit TYPES command" do
    clear_schema
    create_noprimarytbl

    send_handshake_commands(protocol_version: 7, accepted_types: nil)

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [noprimarytbl_def_v7]}]
  end
end
