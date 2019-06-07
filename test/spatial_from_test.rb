require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SpatialFromTest < KitchenSync::EndpointTestCase
  include TestTableSchemas

  def from_or_to
    :from
  end

  def before
    cleanup_spatialtbl
    uninstall_spatial_support
    clear_schema
    install_spatial_support
    create_spatialtbl
  end

  def after
    spawner.stop_binary # done automatically by teardown, but we need to end the transaction before we can remove the extension
    cleanup_spatialtbl
    uninstall_spatial_support
  end

  def force_long_lat_option
    ", 'axis-order=long-lat'" if spatial_axis_order_depends_on_srs?
  end

  test_each "extracts the schema for spatial types" do
    send_handshake_commands

    send_command   Commands::SCHEMA
    expect_command Commands::SCHEMA,
                   [{"tables" => [spatial_reference_table_def, spatialtbl_def].compact}]
  end

  test_each "retrieves row data without SRIDs in WKB format with a 4-byte zero SRID prefix" do
    execute "INSERT INTO spatialtbl VALUES (1, ST_GeomFromText('POINT(10 20)'), ST_GeomFromText('POINT(20 30)')), " \
                                          "(2, ST_GeomFromText('GEOMETRYCOLLECTION(LINESTRING(1.5 2.25,3.125 4.0625),MULTIPOLYGON(((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5))))'), NULL)"
    @rows = [[1, ["00000000010100000000000000000024400000000000003440"].pack("H*"), ["00000000010100000000000000000034400000000000003E40"].pack("H*")],
             [2, ["00000000010700000002000000010200000002000000000000000000F83F000000000000024000000000000009400000000000401040010600000002000000010300000001000000040000000000000000003E40000000000000344000000000008046400000000000004440000000000000244000000000000044400000000000003E400000000000003440010300000001000000050000000000000000002E4000000000000014400000000000004440000000000000244000000000000024400000000000003440000000000000144000000000000024400000000000002E400000000000001440"].pack("H*"), nil]]

    send_handshake_commands

    send_command   Commands::ROWS, ["spatialtbl", [], []]
    expect_command Commands::ROWS,
                   ["spatialtbl", [], []],
                   *@rows
  end

  test_each "retrieves row data with SRIDs in WKB format with a 4-byte SRID prefix" do
    execute "INSERT INTO spatialtbl VALUES (1, ST_GeomFromText('POINT(10 20)', 4326#{force_long_lat_option}), ST_GeomFromText('POINT(20 30)', 4326#{force_long_lat_option})), " \
                                          "(2, ST_GeomFromText('GEOMETRYCOLLECTION(LINESTRING(1.5 2.25,3.125 4.0625),MULTIPOLYGON(((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5))))', 4326#{force_long_lat_option}), NULL)"
    @rows = [[1, ["E6100000010100000000000000000024400000000000003440"].pack("H*"), ["E6100000010100000000000000000034400000000000003E40"].pack("H*")],
             [2, ["E6100000010700000002000000010200000002000000000000000000F83F000000000000024000000000000009400000000000401040010600000002000000010300000001000000040000000000000000003E40000000000000344000000000008046400000000000004440000000000000244000000000000044400000000000003E400000000000003440010300000001000000050000000000000000002E4000000000000014400000000000004440000000000000244000000000000024400000000000003440000000000000144000000000000024400000000000002E400000000000001440"].pack("H*"), nil]]

    send_handshake_commands

    send_command   Commands::ROWS, ["spatialtbl", [], []]
    expect_command Commands::ROWS,
                   ["spatialtbl", [], []],
                   *@rows
  end
end
