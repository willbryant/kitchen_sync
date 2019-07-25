require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class ColumnTypesToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  test_each "accepts the appropriate representation of the column values if an encoding has been defined for that type, otherwise uses strings" do
    clear_schema
    create_misctbl

    execute %Q{INSERT INTO misctbl (pri, boolfield, datefield, timefield, datetimefield, floatfield, doublefield, decimalfield, vchrfield, fchrfield, uuidfield, textfield, blobfield, jsonfield, enumfield) VALUES
                                   (-21, true, '2099-12-31', '12:34:56', '2014-04-13 01:02:03', 1.25, 0.5, 123456.4321, 'vartext', 'fixedtext', 'e23d5cca-32b7-4fb7-917f-d46d01fbff42', 'sometext', 'test', '{"one": 1, "two": "test"}', 'green')} # insert the first row but not the second
    @rows = [[-21,  true, Date.parse('2099-12-31'), TimeOnlyWrapper.new('12:34:56'), Time.parse('2014-04-13 01:02:03'), HashAsStringWrapper.new(1.25), HashAsStringWrapper.new(0.5), BigDecimal.new('123456.4321'), 'vartext', 'fixedtext', 'e23d5cca-32b7-4fb7-917f-d46d01fbff42', 'sometext', 'test',           '{"one": 1, "two": "test"}', 'green'],
             [ 42, false, Date.parse('1900-01-01'), TimeOnlyWrapper.new('00:00:00'), Time.parse('1970-02-03 23:59:59'), HashAsStringWrapper.new(1.25), HashAsStringWrapper.new(0.5), BigDecimal.new('654321.1234'), 'vartext', 'fixedtext', 'c26ae0c4-b071-4058-9044-92042d6740fc', 'sometext', "binary\001test", '{"somearray": [1, 2, 3]}',  "with'quote"]]
    @keys = @rows.collect {|row| [row[0]]}

    expect_handshake_commands
    expect_command Commands::SCHEMA
    send_command   Commands::SCHEMA, ["tables" => [misctbl_def]]
    expect_sync_start_commands
    expect_command Commands::RANGE, ["misctbl"]
    send_command   Commands::RANGE, ["misctbl", @keys[0], @keys[-1]]

    # check that it can handle the various data types without crashing
    expect_command Commands::ROWS,
                   ["misctbl", @keys[0], @keys[-1]]
    send_results   Commands::ROWS,
                   ["misctbl", @keys[0], @keys[-1]],
                   *@rows[1..-1]
    expect_command Commands::HASH,
                   ["misctbl", [], @keys[0], 1]
    send_results   Commands::HASH,
                   ["misctbl", [], @keys[0], 1, 1, hash_of(@rows[0..0])]
    expect_quit_and_close

    assert_equal @rows,
                 query("SELECT * FROM misctbl ORDER BY pri")
  end
end
