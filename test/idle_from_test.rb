require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class IdleFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  test_each "does nothing, and allows other commands after" do
    clear_schema
    create_footbl # arbitrary, just something to show the schema was loaded successfully

    send_handshake_commands
    send_command   Commands::IDLE
    expect_command Commands::IDLE

    send_command   Commands::RANGE, ["footbl"]
    expect_command Commands::RANGE,
                   ["footbl", [], []]
  end
end
