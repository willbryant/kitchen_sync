require 'rubygems'

require 'test/unit'
require 'fileutils'
require 'time'

require 'msgpack'
require 'openssl'
require 'ruby-xxhash'

ENDPOINT_DATABASES = {}
require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper_postgresql')) if ENV['ENDPOINT_DATABASES'].nil? || ENV['ENDPOINT_DATABASES'].include?('postgresql')
require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper_mysql'))      if ENV['ENDPOINT_DATABASES'].nil? || ENV['ENDPOINT_DATABASES'].include?('mysql')

require File.expand_path(File.join(File.dirname(__FILE__), 'kitchen_sync_spawner'))
require File.expand_path(File.join(File.dirname(__FILE__), 'test_table_schemas'))

FileUtils.mkdir_p(File.join(File.dirname(__FILE__), 'tmp'))

class Date
  def to_s
    strftime("%Y-%m-%d") # make the standard input format the output format
  end

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end

class Time
  def to_s
    strftime("%Y-%m-%d %H:%M:%S") # not interested in %z for tests
  end

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end

class Object
  def try!(method, *args)
    send(method, *args)
  end
end

class NilClass
  def try!(method, *args)
    nil
  end
end

module ColumnTypes
  BLOB = "BLOB"
  TEXT = "TEXT"
  VCHR = "VARCHAR"
  FCHR = "CHAR"
  UUID = "UUID"
  BOOL = "BOOL"
  SINT = "INT"
  UINT = "INT UNSIGNED"
  REAL = "REAL"
  DECI = "DECIMAL"
  DATE = "DATE"
  TIME = "TIME"
  DTTM = "DATETIME"

  UNKN = "UNKNOWN"
end

module Commands
  ROWS = 2
  HASH = 7
  RANGE = 8

  PROTOCOL = 32
  EXPORT_SNAPSHOT  = 33
  IMPORT_SNAPSHOT  = 34
  UNHOLD_SNAPSHOT  = 35
  WITHOUT_SNAPSHOT = 36
  SCHEMA = 37
  TARGET_BLOCK_SIZE = 38
  HASH_ALGORITHM = 39
  FILTERS = 40
  QUIT = 0
end

module HashAlgorithm
  MD5 = 0
  XXH64 = 1
end

module PrimaryKeyType
  NO_AVAILABLE_KEY = 0
  EXPLICIT_PRIMARY_KEY = 1
  SUITABLE_UNIQUE_KEY = 2
end

Verbs = Commands.constants.each_with_object({}) {|k, results| results[Commands.const_get(k)] = k.to_s.downcase}.freeze

module KitchenSync
  class TestCase < Test::Unit::TestCase
    EARLIEST_PROTOCOL_VERSION_SUPPORTED = 7
    CURRENT_PROTOCOL_VERSION_USED = 7
    LATEST_PROTOCOL_VERSION_SUPPORTED = 7

    def protocol_version_supported
      LATEST_PROTOCOL_VERSION_SUPPORTED
    end

    undef_method :default_test if instance_methods.include? 'default_test' or
                                  instance_methods.include? :default_test

    def captured_stderr_filename
      @captured_stderr_filename ||= File.join(File.dirname(__FILE__), 'tmp', 'captured_stderr') unless ENV['NO_CAPTURE_STDERR'].to_i > 0
    end

    def binary_path
      @binary_path ||= File.join(File.dirname(__FILE__), '..', 'build', binary_name)
    end

    def spawner
      @spawner ||= KitchenSyncSpawner.new(binary_path, program_args, program_env, :capture_stderr_in => captured_stderr_filename).tap(&:start_binary)
    end

    def unpacker
      spawner.unpacker
    end

    def read_command
      spawner.read_command
    end

    def expect_command(*args)
      command = read_command
      raise "expected command #{args.inspect} but received #{command.inspect}" unless args == command # use this instead of assert_equal so we get the backtrace
    rescue EOFError
      fail "expected #{args.inspect} but the connection was closed; stderr: #{spawner.stderr_contents}"
    end

    def send_command(*args)
      spawner.send_command(*args)
    end

    def send_results(*args)
      spawner.send_results(*args)
    end

    def send_handshake_commands(target_minimum_block_size = 1, hash_algorithm = HashAlgorithm::MD5)
      send_protocol_command
      send_without_snapshot_command
      send_hash_algorithm_command(hash_algorithm)
    end

    def send_protocol_command
      send_command   Commands::PROTOCOL, [protocol_version_supported]
      expect_command Commands::PROTOCOL, [protocol_version_supported]
    end

    def send_without_snapshot_command
      send_command   Commands::WITHOUT_SNAPSHOT
      expect_command Commands::WITHOUT_SNAPSHOT
    end

    def send_hash_algorithm_command(hash_algorithm)
      send_command   Commands::HASH_ALGORITHM, [hash_algorithm]
      expect_command Commands::HASH_ALGORITHM, [hash_algorithm]
    end

    def expect_handshake_commands
      # checking how protocol versions are handled is covered in protocol_versions_test; here we just need to get past that to get on to the commands we want to test
      expect_command Commands::PROTOCOL, [CURRENT_PROTOCOL_VERSION_USED]
      @protocol_version = [CURRENT_PROTOCOL_VERSION_USED, protocol_version_supported].min
      send_command   Commands::PROTOCOL, [@protocol_version]

      # since we haven't asked for multiple workers, we'll always get sent the snapshot-less start command
      expect_command Commands::WITHOUT_SNAPSHOT
      send_command   Commands::WITHOUT_SNAPSHOT
    end

    def expect_sync_start_commands(hash_algorithm = HashAlgorithm::MD5)
      assert_equal   Commands::HASH_ALGORITHM, read_command.first
      send_command   Commands::HASH_ALGORITHM, [hash_algorithm]
    end

    def expect_quit_and_close
      expect_command Commands::QUIT
      assert_equal "", spawner.read_from_program
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
      @program_args ||= [ from_or_to.to_s ]
    end

    def program_env
      @program_env ||= {
        "ENDPOINT_DATABASE_HOST" => database_host,
        "ENDPOINT_DATABASE_PORT" => database_port,
        "ENDPOINT_DATABASE_NAME" => database_name,
        "ENDPOINT_DATABASE_USERNAME" => database_username,
        "ENDPOINT_DATABASE_PASSWORD" => database_password,

        # we force the block size down to 1 so we can test out our algorithms row-by-row, but real runs would use a bigger size
        "ENDPOINT_TARGET_MINIMUM_BLOCK_SIZE" => "1",
      }
    end

    def connection
      @connection ||= @database_settings[:connect].call(database_host, database_port, database_name, database_username, database_password)
    end

    def execute(sql)
      connection.execute(sql)
    end

    def query(sql)
      connection.query(sql).collect {|row| row.values}
    end

    def clear_schema
      connection.views.each {|view_name| execute "DROP VIEW #{view_name}"}
      connection.tables.each {|table_name| execute "DROP TABLE #{table_name}"}
    end

    def hash_of(rows, hash_algorithm = HashAlgorithm::MD5)
      data = rows.collect(&:to_msgpack).join

      case hash_algorithm
      when HashAlgorithm::MD5
        OpenSSL::Digest::MD5.new.digest(data)

      when HashAlgorithm::XXH64
        result = XXhash.xxh64(data)
        [result >> 32, result & 0xFFFFFFFF].pack("NN")
      end
    end

    def hash_and_count_of(rows)
      [hash_of(rows), rows.size]
    end

    def self.test_each(description, only: nil, &block)
      ENDPOINT_DATABASES.each do |database_server, settings|
        next if only && only.to_s != database_server
        define_method("test #{description} for #{database_server}".gsub(/\W+/,'_').to_sym) do
          @database_server = database_server
          @database_settings = settings
          begin
            skip "pending" unless block
            before if respond_to?(:before)
            instance_eval(&block)
            after if respond_to?(:after)
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
