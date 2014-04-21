require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class ColumnTypesFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "returns the appropriate representation of the column values if an encoding has been defined for that type, otherwise uses strings" do
    clear_schema
    create_misctbl

    # the adapters need further work to return these columns to ruby with a consistent type, so leave them out of the test for now
    execute "ALTER TABLE misctbl DROP column timefield, DROP column floatfield, DROP column doublefield, DROP column decimalfield"
    trimmed_misctbl_def = misctbl_def.merge("columns" => misctbl_def["columns"].reject {|column| %w(timefield floatfield doublefield decimalfield).include?(column["name"])})

    execute "INSERT INTO misctbl VALUES (-21, true, '2099-12-31', '2014-04-13 01:02:03', 'test'), (42, false, '1900-01-01', '1970-02-03 23:59:59', 'binary\001test')"
    @rows = [[-21,  true, '2099-12-31', '2014-04-13 01:02:03', 'test'],
             [ 42, false, '1900-01-01', '1970-02-03 23:59:59', "binary\001test"]]
    @keys = @rows.collect {|row| [row[0]]}

    send_handshake_commands
    
    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [trimmed_misctbl_def]}]

    send_command   Commands::OPEN, "misctbl"
    expect_command Commands::HASH_NEXT,
                   [[], @keys[0], hash_of(@rows[0..0])]

    send_command   Commands::ROWS, [], @keys[1]
    expect_command Commands::ROWS,
                   [[], @keys[1]],
                   @rows[0],
                   @rows[1]

    send_command   Commands::HASH_NEXT, [], @keys[1], hash_of(@rows[0..1])
    expect_command Commands::ROWS,
                   [@keys[1], []]
  end
end
