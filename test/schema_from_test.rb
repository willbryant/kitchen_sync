require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "returns an empty list of tables on an empty database" do
    clear_schema

    send_protocol_command
    assert_equal({"tables" => []}, send_command("schema"))
  end

  test_each "returns the name of the tables and the names of their columns" do
    clear_schema
    create_footbl
    create_secondtbl
    send_protocol_command
    
    assert_equal(
      {"tables" => [footbl_def, secondtbl_def]},
      send_command("schema"))
  end

  test_each "selects the first unique key with no nullable columns if there is no primary key" do
    clear_schema
    create_noprimarytbl
    send_protocol_command

    assert_equal(
      {"tables" => [noprimarytbl_def]},
      send_command("schema"))
  end
end
