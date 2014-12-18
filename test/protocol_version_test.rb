require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class ProtocolVersionTest < KitchenSync::EndpointTestCase
  EARLIEST_PROTOCOL_VERSION_SUPPORTED = 4
  LATEST_PROTOCOL_VERSION_SUPPORTED = 4

  def from_or_to
    :from
  end

  test_each "gives back the current version if the client supports it" do
    clear_schema
    send_command   Commands::PROTOCOL, LATEST_PROTOCOL_VERSION_SUPPORTED
    expect_command Commands::PROTOCOL, [LATEST_PROTOCOL_VERSION_SUPPORTED]
  end

  test_each "gives back its version if the client supports a later version" do
    clear_schema
    send_command   Commands::PROTOCOL, LATEST_PROTOCOL_VERSION_SUPPORTED + 1
    expect_command Commands::PROTOCOL, [LATEST_PROTOCOL_VERSION_SUPPORTED]
  end

  test_each "gives back the clients version if the client only supports an earlier version" do
    clear_schema
    send_command   Commands::PROTOCOL, EARLIEST_PROTOCOL_VERSION_SUPPORTED
    expect_command Commands::PROTOCOL, [EARLIEST_PROTOCOL_VERSION_SUPPORTED]
  end if EARLIEST_PROTOCOL_VERSION_SUPPORTED < LATEST_PROTOCOL_VERSION_SUPPORTED
end
