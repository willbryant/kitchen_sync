require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SpatialToTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :to
  end

  def before
    program_env["ENDPOINT_ONLY_TABLES"] = "spatialtbl"
    remove_spatialtbl
    connection.uninstall_spatial_support
    clear_schema
    connection.install_spatial_support
    create_spatialtbl
  end

  def after
    spawner.stop_binary # done automatically by teardown, but we need to end the transaction before we can remove the extension
    remove_spatialtbl # we do this explicitly as otherwise we need to use a force-uninstall option on postgis, which in some situations blocks on locks in a way this doesn't
    connection.uninstall_spatial_support
  end

  def force_long_lat_option
    ", 'axis-order=long-lat'" if connection.spatial_axis_order_depends_on_srs?
  end

  test_each "creates spatial tables if they don't exist" do
    expect_handshake_commands(schema: {"tables" => [spatialtbl_def]})
    read_command
    assert connection.tables.include?("spatialtbl")
    assert_equal nil, connection.table_srids("spatialtbl")["plainspat"]
  end

  test_each "creates spatial tables with SRID settings on columns when supported by the database" do
    omit "This database doesn't support SRID settings on columns" unless connection.schema_srid_settings?
    expect_handshake_commands(schema: {"tables" => [spatialtbl_def(srid: 4326)]})
    read_command
    assert connection.tables.include?("spatialtbl")
    assert_equal "4326", connection.table_srids("spatialtbl")["plainspat"]
  end

  test_each "runs on empty spatial tables" do
    expect_handshake_commands(schema: {"tables" => [spatialtbl_def]})
    expect_command Commands::RANGE, ["spatialtbl"]
    send_command   Commands::RANGE, ["spatialtbl", [], []]
    expect_quit_and_close

    assert_equal [],
                 query("SELECT id, ST_SRID(plainspat) AS plainspat_stid, ST_AsBinary(plainspat#{force_long_lat_option}) AS paingeom_wkb, ST_SRID(pointspat) AS pointspat_srid, ST_AsBinary(pointspat#{force_long_lat_option}) AS pointspat_wkb FROM spatialtbl ORDER BY id")
  end

  test_each "retrieves and saves row data without SRIDs in WKB format with no SRID specified" do
    @rows = [[1, ["010100000000000000000024400000000000003440"].pack("H*"), ["010100000000000000000034400000000000003E40"].pack("H*")],
             [2, ["010700000002000000010200000002000000000000000000F83F000000000000024000000000000009400000000000401040010600000002000000010300000001000000040000000000000000003E40000000000000344000000000008046400000000000004440000000000000244000000000000044400000000000003E400000000000003440010300000001000000050000000000000000002E4000000000000014400000000000004440000000000000244000000000000024400000000000003440000000000000144000000000000024400000000000002E400000000000001440"].pack("H*"), nil]]

    expect_handshake_commands(schema: {"tables" => [spatialtbl_def]})
    expect_command Commands::RANGE, ["spatialtbl"]
    send_command   Commands::RANGE, ["spatialtbl", [1], [2]]
    expect_command Commands::ROWS,
                   ["spatialtbl", [], [2]]
    send_results   Commands::ROWS,
                   ["spatialtbl", [], [2]],
                   *@rows
    expect_quit_and_close

    assert_equal [[1, 0, ["010100000000000000000024400000000000003440"].pack("H*"), 0, ["010100000000000000000034400000000000003E40"].pack("H*")],
                  [2, 0, ["010700000002000000010200000002000000000000000000F83F000000000000024000000000000009400000000000401040010600000002000000010300000001000000040000000000000000003E40000000000000344000000000008046400000000000004440000000000000244000000000000044400000000000003E400000000000003440010300000001000000050000000000000000002E4000000000000014400000000000004440000000000000244000000000000024400000000000003440000000000000144000000000000024400000000000002E400000000000001440"].pack("H*"), nil, nil]],
                 query("SELECT id, ST_SRID(plainspat) AS plainspat_stid, ST_AsBinary(plainspat#{force_long_lat_option}) AS paingeom_wkb, ST_SRID(pointspat) AS pointspat_srid, ST_AsBinary(pointspat#{force_long_lat_option}) AS pointspat_wkb FROM spatialtbl ORDER BY id")
  end

  test_each "retrieves and saves row data with SRIDs in WKB format with a 4-byte SRID prefix" do
    @rows = [[1, ["0101000020E610000000000000000024400000000000003440"].pack("H*"), ["0101000020E610000000000000000034400000000000003E40"].pack("H*")],
             [2, ["0107000020E610000002000000010200000002000000000000000000F83F000000000000024000000000000009400000000000401040010600000002000000010300000001000000040000000000000000003E40000000000000344000000000008046400000000000004440000000000000244000000000000044400000000000003E400000000000003440010300000001000000050000000000000000002E4000000000000014400000000000004440000000000000244000000000000024400000000000003440000000000000144000000000000024400000000000002E400000000000001440"].pack("H*"), nil]]

    expect_handshake_commands(schema: {"tables" => [spatialtbl_def]})
    expect_command Commands::RANGE, ["spatialtbl"]
    send_command   Commands::RANGE, ["spatialtbl", [1], [2]]
    expect_command Commands::ROWS,
                   ["spatialtbl", [], [2]]
    send_results   Commands::ROWS,
                   ["spatialtbl", [], [2]],
                   *@rows
    expect_quit_and_close

    assert_equal [[1, 4326, ["010100000000000000000024400000000000003440"].pack("H*"), 4326, ["010100000000000000000034400000000000003E40"].pack("H*")],
                  [2, 4326, ["010700000002000000010200000002000000000000000000F83F000000000000024000000000000009400000000000401040010600000002000000010300000001000000040000000000000000003E40000000000000344000000000008046400000000000004440000000000000244000000000000044400000000000003E400000000000003440010300000001000000050000000000000000002E4000000000000014400000000000004440000000000000244000000000000024400000000000003440000000000000144000000000000024400000000000002E400000000000001440"].pack("H*"), nil, nil]],
                 query("SELECT id, ST_SRID(plainspat) AS plainspat_stid, ST_AsBinary(plainspat#{force_long_lat_option}) AS paingeom_wkb, ST_SRID(pointspat) AS pointspat_srid, ST_AsBinary(pointspat#{force_long_lat_option}) AS pointspat_wkb FROM spatialtbl ORDER BY id")
  end
end
