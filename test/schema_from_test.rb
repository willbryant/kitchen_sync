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
    expected_row_data = adapterspecifictbl_row
    execute "INSERT INTO #{connection.quote_ident adapterspecifictbl_def["name"]} (#{expected_row_data.keys.collect {|k| connection.quote_ident k}.join(", ")}) VALUES (#{expected_row_data.values.collect {|v| "'#{connection.escape v.to_s}'"}.join(", ")})"

    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [adapterspecifictbl_def]}]

    send_command   Commands::ROWS, [adapterspecifictbl_def["name"], [], []]
    expected_command = [Commands::ROWS, [adapterspecifictbl_def["name"], [], []]]
    command = read_command
    raise "expected command followed by one row but received #{command.inspect}" unless command.size == expected_command.size + 1
    row_data = command.pop
    raise "expected command #{expected_command.inspect} but received #{command.inspect}" unless expected_command == command
    expected_row_data.each do |column_name, value|
      if column_name == "pri"
        assert_equal 1, value # auto-increment should start at 1 for a new table
      else
        column_index = adapterspecifictbl_def["columns"].index { |column_def| column_def["name"] == column_name }
        assert_equal value, row_data[column_index]
      end
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

  test_each "reports unsupported column types" do
    clear_schema
    create_unsupportedtbl

    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [unsupportedtbl_def]}]
  end
end
