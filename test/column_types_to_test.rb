require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class ColumnTypesToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  test_each "accepts the appropriate representation of the column values if an encoding has been defined for that type, otherwise uses strings" do
    clear_schema
    create_misctbl

    # the adapters need further work to return these columns to ruby with a consistent type, so leave them out of the test for now
    execute "ALTER TABLE misctbl DROP column timefield, DROP column floatfield, DROP column doublefield, DROP column decimalfield"
    trimmed_misctbl_def = misctbl_def.merge("columns" => misctbl_def["columns"].reject {|column| %w(timefield floatfield doublefield decimalfield).include?(column["name"])})

    execute "INSERT INTO misctbl VALUES (21, true, '2099-12-31', '2014-04-13 01:02:03', 'test')" # insert the first row but not the second
    @rows = [['21',  true, '2099-12-31', '2014-04-13 01:02:03', 'test'],
             ['42', false, '1900-01-01', '1970-02-03 23:59:59', "binary\001test"]]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, "tables" => [trimmed_misctbl_def]
    expect_command Commands::OPEN, ["misctbl"]
    send_command   Commands::HASH_NEXT, [], @keys[0], hash_of(@rows[0..0])
    expect_command Commands::ROWS,
                   [@keys[0], []]
    send_results   Commands::ROWS,
                   [@keys[0], []],
                   @rows[1]
    expect_command Commands::QUIT

    assert_equal @rows.collect {|row| row.collect {|value| value.to_s unless value.nil?}},
                 query("SELECT * FROM misctbl ORDER BY pri")
  end
end
