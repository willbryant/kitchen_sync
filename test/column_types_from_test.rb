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

    execute %Q{INSERT INTO misctbl (pri, boolfield, datefield, datetimefield, vchrfield, fchrfield, uuidfield, textfield, blobfield, jsonfield) VALUES
                                   (-21, true, '2099-12-31', '2014-04-13 01:02:03', 'vartext', 'fixedtext', 'e23d5cca-32b7-4fb7-917f-d46d01fbff42', 'sometext', 'test', '{"one": 1, "two": "test"}'),
                                   (42, false, '1900-01-01', '1970-02-03 23:59:59', 'vartext', 'fixedtext', 'c26ae0c4-b071-4058-9044-92042d6740fc', 'sometext', 'binary\001test', '{"somearray": [1, 2, 3]}')}
    @rows = [[-21,  true, '2099-12-31', '2014-04-13 01:02:03', 'vartext', 'fixedtext', 'e23d5cca-32b7-4fb7-917f-d46d01fbff42', 'sometext', 'test',           '{"one": 1, "two": "test"}'],
             [ 42, false, '1900-01-01', '1970-02-03 23:59:59', 'vartext', 'fixedtext', 'c26ae0c4-b071-4058-9044-92042d6740fc', 'sometext', "binary\001test", '{"somearray": [1, 2, 3]}']]
    @keys = @rows.collect {|row| [row[0]]}

    send_handshake_commands
    
    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [trimmed_misctbl_def]}]

    send_command   Commands::HASH, ["misctbl", [], @keys[0], 1000]
    expect_command Commands::HASH,
                   ["misctbl", [], @keys[0], 1000, 1, hash_of(@rows[0..0])]

    send_command   Commands::ROWS, ["misctbl", [], @keys[1]]
    expect_command Commands::ROWS,
                   ["misctbl", [], @keys[1]],
                   @rows[0],
                   @rows[1]

    send_command   Commands::HASH, ["misctbl", [], @keys[1], 1000]
    expect_command Commands::HASH,
                   ["misctbl", [], @keys[1], 1000, 2, hash_of(@rows[0..1])]
  end
end
