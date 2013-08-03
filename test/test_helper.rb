require 'rubygems'

require 'test/unit'
require 'fileutils'
require 'mocha/setup'

require 'msgpack'
require 'pg'
require 'mysql2'

require File.expand_path(File.join(File.dirname(__FILE__), 'kitchen_sync_spawner'))
require File.expand_path(File.join(File.dirname(__FILE__), 'test_table_schemas'))

FileUtils.mkdir_p(File.join(File.dirname(__FILE__), 'tmp'))

class PGconn
  def execute(sql)
    async_exec(sql)
  end

  def tables
    query("SELECT tablename FROM pg_tables WHERE schemaname = ANY (current_schemas(false))").collect {|row| row["tablename"]}
  end
end

class Mysql2::Client
  def execute(sql)
    query(sql)
  end

  def tables
    query("SHOW TABLES").collect {|row| row.values.first}
  end
end

ENDPOINT_DATABASES = {
  "postgresql" => {
    :connect => lambda { |host, port, name, username, password| PGconn.connect(
      host,
      port,
      nil,
      nil,
      name,
      username,
      password)
    }
  },
  "mysql" => {
    :connect => lambda { |host, port, name, username, password| Mysql2::Client.new(
      :host     => host,
      :port     => port.to_i,
      :database => name,
      :username => username,
      :password => password)
    }
  }
}

module KitchenSync
  class TestCase < Test::Unit::TestCase
    CURRENT_PROTOCOL_VERSION = 1

    undef_method :default_test if instance_methods.include? 'default_test' or
                                  instance_methods.include? :default_test

    def captured_stderr_filename
      @captured_stderr_filename ||= File.join(File.dirname(__FILE__), 'tmp', 'captured_stderr')
    end

    def binary_path
      @binary_path ||= File.join(File.dirname(__FILE__), '..', 'build', binary_name)
    end

    def spawner
      @spawner ||= KitchenSyncSpawner.new(binary_path, program_args, :capture_stderr_in => captured_stderr_filename).tap(&:start_binary)
    end

    def send_command(*args)
      spawner.send_command(*args)
    end

    def send_protocol_command
      assert_equal CURRENT_PROTOCOL_VERSION, send_command("protocol", CURRENT_PROTOCOL_VERSION)
    end

    def receive_commands(*args)
      spawner.receive_commands(*args) do |command|
        send(*command)
      end
    end

    def expect_stderr(contents)
      spawner.expect_stderr(contents) { yield }
    end
    
    def fixture_file_path(filename)
      File.join(File.dirname(__FILE__), 'fixtures', filename)
    end
  end

  class EndpointTestCase < TestCase
    def binary_name
      "ks_#{@database_server}"
    end

    def database_host
      ENV["#{@database_server.upcase}_DATABASE_HOST"]     || ENV["ENDPOINT_DATABASE_HOST"]     || ""
    end

    def database_port
      ENV["#{@database_server.upcase}_DATABASE_PORT"]     || ENV["ENDPOINT_DATABASE_PORT"]     || ""
    end

    def database_name
      ENV["#{@database_server.upcase}_DATABASE_NAME"]     || ENV["ENDPOINT_DATABASE_NAME"]     || "ks_test"
    end

    def database_username
      ENV["#{@database_server.upcase}_DATABASE_USERNAME"] || ENV["ENDPOINT_DATABASE_USERNAME"] || ""
    end

    def database_password
      ENV["#{@database_server.upcase}_DATABASE_PASSWORD"] || ENV["ENDPOINT_DATABASE_PASSWORD"] || ""
    end

    def program_args
      [ from_or_to.to_s, database_host, database_port, database_name, database_username, database_password ]
    end

    def connection
      @connection ||= @database_settings[:connect].call(database_host, database_port, database_name, database_username, database_password)
    end

    def execute(sql)
      connection.execute sql
    end

    def clear_schema
      connection.tables.each {|table_name| execute "DROP TABLE #{table_name}"}
    end

    def self.test_each(description, &block)
      ENDPOINT_DATABASES.each do |database_server, settings|
        define_method("test #{description} for #{database_server}".gsub(/\W+/,'_').to_sym) do
          @database_server = database_server
          @database_settings = settings
          begin
            instance_eval(&block)
          ensure
            @spawner.stop_binary if @spawner
            @spawner = nil
            @connection.close rescue nil if @connection
          end
        end
      end
    end
  end
end
