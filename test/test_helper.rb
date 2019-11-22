require 'rubygems'

require 'test/unit'
require 'fileutils'
require 'time'

require 'msgpack'
require 'openssl'
require 'ruby-xxhash'

ENDPOINT_ADAPTERS = {}
require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper_postgresql')) if ENV['ENDPOINT_DATABASES'].nil? || ENV['ENDPOINT_DATABASES'].include?('postgresql')
require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper_mysql'))      if ENV['ENDPOINT_DATABASES'].nil? || ENV['ENDPOINT_DATABASES'].include?('mysql')

require File.expand_path(File.join(File.dirname(__FILE__), 'kitchen_sync_spawner'))
require File.expand_path(File.join(File.dirname(__FILE__), 'test_table_schemas'))
require File.expand_path(File.join(File.dirname(__FILE__), 'basic_type_formatters'))

FileUtils.mkdir_p(File.join(File.dirname(__FILE__), 'tmp'))

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

module LegacyColumnType
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
  ENUM = "ENUM"
end

module ColumnType
  UNKNOWN = "unknown"
  MYSQL_SPECIFIC = "mysql_specific"
  POSTGRESQL_SPECIFIC = "postgresql_specific"
  BINARY = "binary"
  BINARY_VARBINARY = "binary.varbinary"
  BINARY_FIXED = "binary.fixed"
  TEXT = "text"
  TEXT_VARCHAR = "text.varchar"
  TEXT_FIXED = "text.fixed"
  JSON = "json"
  JSON_BINARY = "json.binary"
  UUID = "uuid"
  BOOLEAN = "boolean"
  SINT_8BIT = "integer.8bit"
  SINT_16BIT = "integer.16bit"
  SINT_24BIT = "integer.24bit"
  SINT_32BIT = "integer"
  SINT_64BIT = "integer.64bit"
  UINT_8BIT = "integer.unsigned.8bit"
  UINT_16BIT = "integer.unsigned.16bit"
  UINT_24BIT = "integer.unsigned.24bit"
  UINT_32BIT = "integer.unsigned"
  UINT_64BIT = "integer.unsigned.64bit"
  FLOAT_64BIT = "float"
  FLOAT_32BIT = "float.32bit"
  DECIMAL = "decimal"
  DATE = "date"
  TIME = "time"
  TIME_TZ = "time.tz"
  DATETIME = "datetime"
  DATETIME_TZ = "datetime.tz"
  DATETIME_MYSQLTIMESTAMP = "datetime.mysqltimestamp"
  SPATIAL = "spatial"
  SPATIAL_GEOGRAPHY = "spatial.geography"
  ENUMERATION = "enum"
end

module Commands
  ROWS = 2
  HASH = 7
  RANGE = 8
  IDLE = 31;

  PROTOCOL = 32
  EXPORT_SNAPSHOT  = 33
  IMPORT_SNAPSHOT  = 34
  UNHOLD_SNAPSHOT  = 35
  WITHOUT_SNAPSHOT = 36
  SCHEMA = 37
  TARGET_BLOCK_SIZE = 38
  HASH_ALGORITHM = 39
  FILTERS = 40
  TYPES = 41
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
  ENTIRE_ROW_AS_KEY = 3
end

module KitchenSync
  class TestCase < Test::Unit::TestCase
    EARLIEST_PROTOCOL_VERSION_SUPPORTED = 7
    CURRENT_PROTOCOL_VERSION_USED = 8
    LATEST_PROTOCOL_VERSION_SUPPORTED = 8

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
      raise "expected command: [#{args.collect {|arg| PP.pp(arg, "", 200).strip}.join(",\n")}]\nbut received: [#{command.collect {|arg| PP.pp(arg, "", 200).strip}.join(",\n")}}]" unless args == command # use this instead of assert_equal so we get the backtrace
    rescue EOFError
      fail "expected #{args.inspect} but the connection was closed; stderr: #{spawner.stderr_contents}"
    end

    def send_command(*args)
      spawner.send_command(*args)
    end

    def send_results(*args)
      spawner.send_results(*args)
    end

    def send_handshake_commands(protocol_version: LATEST_PROTOCOL_VERSION_SUPPORTED, target_minimum_block_size: 1, hash_algorithm: HashAlgorithm::MD5, filters: nil, accepted_types: connection.supported_column_types)
      send_protocol_command(protocol_version)
      send_hash_algorithm_command(hash_algorithm)
      send_filters_command(filters) if filters
      send_types_command(accepted_types) if accepted_types
      send_without_snapshot_command
    end

    def send_protocol_command(protocol_version)
      send_command   Commands::PROTOCOL, [protocol_version]
      expect_command Commands::PROTOCOL, [protocol_version]
    end

    def send_without_snapshot_command
      send_command   Commands::WITHOUT_SNAPSHOT
      expect_command Commands::WITHOUT_SNAPSHOT
    end

    def send_hash_algorithm_command(hash_algorithm)
      send_command   Commands::HASH_ALGORITHM, [hash_algorithm]
      expect_command Commands::HASH_ALGORITHM, [hash_algorithm]
    end

    def send_filters_command(filters)
      send_command   Commands::FILTERS, [filters]
      expect_command Commands::FILTERS
    end

    def send_types_command(accepted_types)
      send_command   Commands::TYPES, [accepted_types]
      expect_command Commands::TYPES
    end

    def expect_handshake_commands(protocol_version_expected: CURRENT_PROTOCOL_VERSION_USED, protocol_version_supported: LATEST_PROTOCOL_VERSION_SUPPORTED, hash_algorithm: HashAlgorithm::MD5, filters: nil, schema:)
      # checking how protocol versions are handled is covered in protocol_versions_test; here we just need to get past that to get on to the commands we want to test
      expect_command Commands::PROTOCOL, [protocol_version_expected]
      @protocol_version = [protocol_version_expected, protocol_version_supported].min
      send_command   Commands::PROTOCOL, [@protocol_version]

      assert_equal   Commands::HASH_ALGORITHM, read_command.first
      send_command   Commands::HASH_ALGORITHM, [hash_algorithm]

      if filters
        expect_command Commands::FILTERS, [filters]
        send_command   Commands::FILTERS
      end

      if @protocol_version > 7
        expect_command Commands::TYPES, [connection.supported_column_types]
        send_command   Commands::TYPES
      end

      # since we haven't asked for multiple workers, we'll always get sent the snapshot-less start command
      expect_command Commands::WITHOUT_SNAPSHOT
      send_command   Commands::WITHOUT_SNAPSHOT

      expect_command Commands::SCHEMA
      send_command   Commands::SCHEMA, [schema]
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

    def program_args
      @program_args ||= [ from_or_to.to_s ]
    end

    def program_env
      @program_env ||= {
        "ENDPOINT_DATABASE_HOST"     => connection.host,
        "ENDPOINT_DATABASE_PORT"     => connection.port,
        "ENDPOINT_DATABASE_NAME"     => connection.name,
        "ENDPOINT_DATABASE_USERNAME" => connection.username,
        "ENDPOINT_DATABASE_PASSWORD" => connection.password,

        # we force the block size down to 1 so we can test out our algorithms row-by-row, but real runs would use a bigger size
        "ENDPOINT_TARGET_MINIMUM_BLOCK_SIZE" => "1",
      }
    end

    def connection
      @connection ||= @adapter_class.new
    end

    def execute(sql)
      connection.execute(sql)
    end

    def query(sql)
      connection.query(sql).collect {|row| row.values}
    end

    def clear_schema
      connection.views.each {|view_name| execute "DROP VIEW #{connection.quote_ident view_name}"}
      connection.tables.each {|table_name| execute "DROP TABLE #{connection.quote_ident table_name}"}
    end

    def hash_of(rows, hash_algorithm = HashAlgorithm::MD5)
      data = rows.collect {|row| MessagePack.pack(row, compatibility_mode: true)}.join

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
      ENDPOINT_ADAPTERS.each do |database_server, adapter_class|
        next if only && only.to_s != database_server
        define_method("test #{database_server} #{description}".gsub(/\W+/,'_').to_sym) do
          @database_server = database_server
          @adapter_class = adapter_class
          begin
            skip "pending" unless block
            before if respond_to?(:before)
            begin
              instance_eval(&block)
            ensure
              after if respond_to?(:after)
            end
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
