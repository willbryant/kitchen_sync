require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class ProtocolVersionTest < KitchenSync::EndpointTestCase
  EARLIEST_PROTOCOL_VERSION_SUPPORTED = 1
  LATEST_PROTOCOL_VERSION_SUPPORTED = 1

  def from_or_to
    :from
  end

  test_each "gives back the current version if the client supports it" do
    assert_equal LATEST_PROTOCOL_VERSION_SUPPORTED, send_command("protocol", LATEST_PROTOCOL_VERSION_SUPPORTED)
  end

  test_each "gives back its version if the client supports a later version" do
    assert_equal LATEST_PROTOCOL_VERSION_SUPPORTED, send_command("protocol", LATEST_PROTOCOL_VERSION_SUPPORTED + 1)
  end

  test_each "gives back the clients version if the client only supports an earlier version" do
    assert_equal EARLIEST_PROTOCOL_VERSION_SUPPORTED, send_command("protocol", EARLIEST_PROTOCOL_VERSION_SUPPORTED)
  end if EARLIEST_PROTOCOL_VERSION_SUPPORTED < LATEST_PROTOCOL_VERSION_SUPPORTED
end
