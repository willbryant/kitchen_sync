require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "accepts the without_snapshot command for solo from endpoints after the protocol negotation, and returns nil to show its completed" do
    clear_schema
    create_footbl # arbitrary, just something to show the schema was loaded successfully

    send_protocol_command
    assert_equal nil, send_command("without_snapshot")
    assert_equal({"tables" => [footbl_def]}, send_command("schema"))
  end

  test_each "gives back a string from the export_snapshot command, and accepts that string in another worker" do
    clear_schema
    create_footbl # arbitrary, just something to show the schema was loaded successfully

    send_protocol_command
    snapshot = send_command("export_snapshot")
    assert_instance_of String, snapshot
    extra_spawner = KitchenSyncSpawner.new(binary_path, program_args, :capture_stderr_in => captured_stderr_filename).tap(&:start_binary)
    begin
      extra_spawner.send_command("protocol", CURRENT_PROTOCOL_VERSION)
      assert_equal nil, extra_spawner.send_command("import_snapshot", snapshot)
      assert_equal({"tables" => [footbl_def]}, extra_spawner.send_command("schema"))
    ensure
      extra_spawner.stop_binary
    end
    assert_equal nil, send_command("unhold_snapshot")
    assert_equal({"tables" => [footbl_def]}, send_command("schema"))
  end
end
