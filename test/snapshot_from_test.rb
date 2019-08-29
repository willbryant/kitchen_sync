require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SnapshotFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def send_schema_command(to = spawner)
    to.send_command Commands::SCHEMA
    assert_equal [Commands::SCHEMA, [{"tables" => [footbl_def]}]], to.read_command
  end

  test_each "accepts the without_snapshot command for solo from endpoints after the protocol negotation, and returns nil to show its completed" do
    clear_schema
    create_footbl # arbitrary, just something to show the schema was loaded successfully

    send_protocol_command(LATEST_PROTOCOL_VERSION_SUPPORTED)
    send_command   Commands::WITHOUT_SNAPSHOT
    expect_command Commands::WITHOUT_SNAPSHOT
    send_schema_command
  end

  test_each "gives back a string from the export_snapshot command, and accepts that string in another worker" do
    clear_schema
    create_footbl # arbitrary, just something to show the schema was loaded successfully

    send_protocol_command(LATEST_PROTOCOL_VERSION_SUPPORTED)
    send_command   Commands::EXPORT_SNAPSHOT
    command, args = read_command
    assert_equal Commands::EXPORT_SNAPSHOT, command
    assert_equal 1, args.length
    snapshot = args[0]
    assert_instance_of String, snapshot

    extra_spawner = KitchenSyncSpawner.new(binary_path, program_args, program_env, :capture_stderr_in => captured_stderr_filename).tap(&:start_binary)
    begin
      extra_spawner.send_command Commands::PROTOCOL, [LATEST_PROTOCOL_VERSION_SUPPORTED]
      assert_equal [Commands::PROTOCOL, [LATEST_PROTOCOL_VERSION_SUPPORTED]], extra_spawner.read_command
      extra_spawner.send_command Commands::IMPORT_SNAPSHOT, [snapshot]
      assert_equal [Commands::IMPORT_SNAPSHOT], extra_spawner.read_command
    
      send_command Commands::UNHOLD_SNAPSHOT
      expect_command Commands::UNHOLD_SNAPSHOT

      send_schema_command
      send_schema_command extra_spawner
    ensure
      extra_spawner.stop_binary
    end
  end
end
