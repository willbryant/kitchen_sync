#!/usr/bin/env ruby

WRITE_WORKERS = 3
READ_WORKERS = 3
YIELD_TIME = 0.01
ADAPTER = ARGV[0] || "Usage: snapshot_precision.rb (mysql|postgresql)"

require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

Adapter = ENDPOINT_ADAPTERS[ADAPTER]

setup_connection = Adapter.new
setup_connection.execute "DROP TABLE IF EXISTS snapshot_test"
setup_connection.execute "CREATE TABLE snapshot_test (id #{setup_connection.identity_column_type} PRIMARY KEY, payload INT)"
setup_connection.close

binary_name = "ks_#{ADAPTER}"
binary_path = File.join(File.dirname(__FILE__), '..', 'build', binary_name)
program_args = ["from"]
program_env = {
  "ENDPOINT_DATABASE_HOST"     => setup_connection.host,
  "ENDPOINT_DATABASE_PORT"     => setup_connection.port,
  "ENDPOINT_DATABASE_NAME"     => setup_connection.name,
  "ENDPOINT_DATABASE_USERNAME" => setup_connection.username,
  "ENDPOINT_DATABASE_PASSWORD" => setup_connection.password,
}
spawners = (1..READ_WORKERS).collect do
  KitchenSyncSpawner.new(binary_path, program_args, program_env)
end

children = (1..WRITE_WORKERS).collect do
  fork do
    begin
      child_connection = Adapter.new
      loop do
        child_connection.execute "BEGIN"
        rand(10).times do
          child_connection.execute "INSERT INTO snapshot_test (payload) VALUES (#{Process.pid})"
        end
        child_connection.execute "COMMIT"
      end
    rescue
      STDERR.puts "worker #{Process.pid} exiting on #{$!}"
      exit 0
    end
  end
end

begin
  parent_connection = Adapter.new

  100.times do |iteration|
    sleep 1
    begin
      parent_connection.execute "DELETE FROM snapshot_test" if iteration % 10 == 0

      spawners.each do |spawner|
        spawner.start_binary
        spawner.send_command Commands::PROTOCOL, [KitchenSync::TestCase::LATEST_PROTOCOL_VERSION_SUPPORTED]
        spawner.read_command
      end

      snapshot = nil
      spawners.each do |spawner|
        sleep YIELD_TIME
        if spawner == spawners.first
          spawner.send_command(Commands::EXPORT_SNAPSHOT)
          snapshot = spawner.read_command.last[0]
        else
          spawner.send_command(Commands::IMPORT_SNAPSHOT, [snapshot])
          spawner.read_command
        end
      end
      spawners.first.send_command(Commands::UNHOLD_SNAPSHOT)
      spawners.first.read_command

      sleep YIELD_TIME
      parent_rows = parent_connection.query("SELECT id, payload FROM snapshot_test ORDER BY id").to_a.collect(&:values)

      row_collections = spawners.collect do |spawner, index|
        sleep YIELD_TIME
        spawner.send_command(Commands::ROWS, ["snapshot_test", [], []])
        _command, _args, *rows = spawner.read_command
        rows
      end

      if row_collections.uniq.size != 1
        puts "test failed - different connection saw different snapshots of the rows!"
        row_collections.each do |rows|
          puts rows.inspect
        end
      elsif row_collections.first == parent_rows
        puts "test indeterminate - different connections saw the same snapshot of the rows, but no more changes had been made since the snapshot anyway"
      else
        puts "test passed - different connections saw the same snapshot of the rows despite subsequent changes"
      end
    ensure
      spawners.each { |spawner| spawner.stop_binary }
    end
  end

  parent_connection.close
ensure
  children.each { |pid| Process.kill("KILL", pid) }
end