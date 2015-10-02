require 'rubygems'

require 'test/unit'
require 'fileutils'
require 'time'

require 'msgpack'
require 'pg'
require 'mysql2'
require 'openssl'
require 'ruby-xxhash'

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

class PGconn
  def execute(sql)
    async_exec(sql)
  end

  def tables
    query("SELECT tablename FROM pg_tables WHERE schemaname = ANY (current_schemas(false)) ORDER BY tablename").collect {|row| row["tablename"]}
  end

  def table_primary_key_name(table_name)
    "#{table_name}_pkey"
  end

  def table_keys(table_name)
    query(<<-SQL).collect {|row| row["relname"]}
      SELECT index_class.relname
        FROM pg_class table_class, pg_index, pg_class index_class
       WHERE table_class.relname = '#{table_name}' AND
             table_class.oid = pg_index.indrelid AND
             index_class.oid = pg_index.indexrelid AND
             index_class.relkind = 'i' AND
             NOT pg_index.indisprimary
    SQL
  end

  def table_keys_unique(table_name)
    query(<<-SQL).each_with_object({}) {|row, results| results[row["relname"]] = row["indisunique"]}
      SELECT index_class.relname, indisunique
        FROM pg_class table_class, pg_index, pg_class index_class
       WHERE table_class.relname = '#{table_name}' AND
             table_class.oid = pg_index.indrelid AND
             index_class.oid = pg_index.indexrelid AND
             index_class.relkind = 'i' AND
             NOT pg_index.indisprimary
    SQL
  end

  def table_key_columns(table_name)
    query(<<-SQL).each_with_object({}) {|row, results| (results[row["relname"]] ||= []) << row["attname"]}
      SELECT index_class.relname, attname
        FROM pg_class table_class, pg_index, pg_class index_class, generate_subscripts(indkey, 1) AS position, pg_attribute
       WHERE table_class.relname = '#{table_name}' AND
             table_class.oid = pg_index.indrelid AND
             index_class.oid = pg_index.indexrelid AND
             index_class.relkind = 'i' AND
             table_class.oid = pg_attribute.attrelid AND
             pg_attribute.attnum = indkey[position]
       ORDER BY position
    SQL
  end

  def table_column_names(table_name)
    query(<<-SQL).collect {|row| row["attname"]}
      SELECT attname
        FROM pg_attribute, pg_class
       WHERE attrelid = pg_class.oid AND
             attnum > 0 AND
             NOT attisdropped AND
             relname = '#{table_name}'
       ORDER BY attnum
    SQL
  end

  def table_column_types(table_name)
    query(<<-SQL).collect.with_object({}) {|row, results| results[row["attname"]] = row["atttype"]}
      SELECT attname, format_type(atttypid, atttypmod) AS atttype
        FROM pg_attribute, pg_class, pg_type
       WHERE attrelid = pg_class.oid AND
             atttypid = pg_type.oid AND
             attnum > 0 AND
             NOT attisdropped AND
             relname = '#{table_name}'
       ORDER BY attnum
    SQL
  end

  def table_column_nullability(table_name)
    query(<<-SQL).collect.with_object({}) {|row, results| results[row["attname"]] = !row["attnotnull"]}
      SELECT attname, attnotnull
        FROM pg_attribute, pg_class
       WHERE attrelid = pg_class.oid AND
             attnum > 0 AND
             NOT attisdropped AND
             relname = '#{table_name}'
       ORDER BY attnum
    SQL
  end

  def table_column_defaults(table_name)
    query(<<-SQL).collect.with_object({}) {|row, results| results[row["attname"]] = row["attdefault"].try!(:gsub, /^'(.*)'::.*$/, '\\1')}
      SELECT attname, (CASE WHEN atthasdef THEN pg_get_expr(adbin, adrelid) ELSE NULL END) AS attdefault
        FROM pg_attribute
        JOIN pg_class ON attrelid = pg_class.oid
        LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum
       WHERE attnum > 0 AND
             NOT attisdropped AND
             relname = '#{table_name}'
       ORDER BY attnum
    SQL
  end

  def table_column_sequences(table_name)
    table_column_defaults(table_name).collect.with_object({}) {|(column, default), results| results[column] = !!(default =~ /^nextval\('\w+_seq'::regclass\)/)}
  end

  def quote_ident(name)
    self.class.quote_ident(name)
  end

  def zero_time_value
    "00:00:00"
  end
end

class Mysql2::Client
  def execute(sql)
    query(sql)
  end

  def tables
    query("SHOW TABLES").collect {|row| row.values.first}
  end

  def table_primary_key_name(table_name)
    "PRIMARY"
  end

  def table_keys(table_name)
    query("SHOW KEYS FROM #{table_name}").collect {|row| row["Key_name"] unless row["Key_name"] == "PRIMARY"}.compact
  end

  def table_keys_unique(table_name)
    query("SHOW KEYS FROM #{table_name}").each_with_object({}) {|row, results| results[row["Key_name"]] = row["Non_unique"].zero? unless row["Key_name"] == "PRIMARY"}
  end

  def table_key_columns(table_name)
    query("SHOW KEYS FROM #{table_name}").each_with_object({}) {|row, results| (results[row["Key_name"]] ||= []) << row["Column_name"]}
  end

  def table_column_names(table_name)
    query("SHOW COLUMNS FROM #{table_name}").collect {|row| row.values.first}.compact
  end

  def table_column_types(table_name)
    query("SHOW COLUMNS FROM #{table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = row["Type"]}
  end

  def table_column_nullability(table_name)
    query("SHOW COLUMNS FROM #{table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = (row["Null"] == "YES")}
  end

  def table_column_defaults(table_name)
    query("SHOW COLUMNS FROM #{table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = row["Default"]}
  end

  def table_column_sequences(table_name)
    query("SHOW COLUMNS FROM #{table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = !!(row["Extra"] =~ /auto_increment/)}
  end

  def quote_ident(name)
    "`#{name}`"
  end

  def zero_time_value
    Time.new(2000, 1, 1, 0, 0, 0)
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
      password).tap {|conn| conn.type_mapping = PG::BasicTypeMapping.new(conn)}
    }
  },
  "mysql" => {
    :connect => lambda { |host, port, name, username, password| Mysql2::Client.new(
      :host     => host,
      :port     => port.to_i,
      :database => name,
      :username => username,
      :password => password,
      :cast_booleans => true)
    }
  }
}

module ColumnTypes
  BLOB = "BLOB"
  TEXT = "TEXT"
  VCHR = "VARCHAR"
  FCHR = "CHAR"
  BOOL = "BOOL"
  SINT = "INT"
  UINT = "INT UNSIGNED"
  REAL = "REAL"
  DECI = "DECIMAL"
  DATE = "DATE"
  TIME = "TIME"
  DTTM = "DATETIME"
end

module Commands
  OPEN = 1
  ROWS = 2
  HASH_NEXT = 3
  HASH_FAIL = 4
  ROWS_AND_HASH_NEXT = 5
  ROWS_AND_HASH_FAIL = 6

  PROTOCOL = 32
  EXPORT_SNAPSHOT  = 33
  IMPORT_SNAPSHOT  = 34
  UNHOLD_SNAPSHOT  = 35
  WITHOUT_SNAPSHOT = 36
  SCHEMA = 37
  TARGET_BLOCK_SIZE = 38
  HASH_ALGORITHM = 39
  QUIT = 0
end

module HashAlgorithm
  MD5 = 0
  XXH64 = 1
end

Verbs = Commands.constants.each_with_object({}) {|k, results| results[Commands.const_get(k)] = k.to_s.downcase}.freeze

module KitchenSync
  class TestCase < Test::Unit::TestCase
    PROTOCOL_VERSION_SUPPORTED = 6

    undef_method :default_test if instance_methods.include? 'default_test' or
                                  instance_methods.include? :default_test

    def captured_stderr_filename
      @captured_stderr_filename ||= File.join(File.dirname(__FILE__), 'tmp', 'captured_stderr') unless ENV['NO_CAPTURE_STDERR'].to_i > 0
    end

    def binary_path
      @binary_path ||= File.join(File.dirname(__FILE__), '..', 'build', binary_name)
    end

    def spawner
      @spawner ||= KitchenSyncSpawner.new(binary_path, program_args, :capture_stderr_in => captured_stderr_filename).tap(&:start_binary)
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
      send_target_minimum_block_size_command(target_minimum_block_size)
      send_hash_algorithm_command(hash_algorithm)
    end

    def send_protocol_command
      send_command   Commands::PROTOCOL, PROTOCOL_VERSION_SUPPORTED
      expect_command Commands::PROTOCOL, [PROTOCOL_VERSION_SUPPORTED]
    end

    def send_without_snapshot_command
      send_command   Commands::WITHOUT_SNAPSHOT
      expect_command Commands::WITHOUT_SNAPSHOT
    end

    def send_target_minimum_block_size_command(target_minimum_block_size)
      send_command   Commands::TARGET_BLOCK_SIZE, target_minimum_block_size
      expect_command Commands::TARGET_BLOCK_SIZE, [target_minimum_block_size]
    end

    def send_hash_algorithm_command(hash_algorithm)
      send_command   Commands::HASH_ALGORITHM, hash_algorithm
      expect_command Commands::HASH_ALGORITHM, [hash_algorithm]
    end

    def expect_handshake_commands(target_minimum_block_size = 1, hash_algorithm = HashAlgorithm::MD5)
      # checking how protocol versions are handled is covered in protocol_versions_test; here we just need to get past that to get on to the commands we want to test
      expect_command Commands::PROTOCOL, [PROTOCOL_VERSION_SUPPORTED]
      send_command   Commands::PROTOCOL, PROTOCOL_VERSION_SUPPORTED

      # we force the block size down to 1 by default so we can test out our algorithms row-by-row, but real runs would use a bigger size
      assert_equal   Commands::TARGET_BLOCK_SIZE, read_command.first
      send_command   Commands::TARGET_BLOCK_SIZE, target_minimum_block_size

      assert_equal   Commands::HASH_ALGORITHM, read_command.first
      send_command   Commands::HASH_ALGORITHM, hash_algorithm

      # since we haven't asked for multiple workers, we'll always get sent the snapshot-less start command
      expect_command Commands::WITHOUT_SNAPSHOT
      send_command   Commands::WITHOUT_SNAPSHOT
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
      @program_args ||= [ from_or_to.to_s, database_host, database_port, database_name, database_username, database_password, "-" ]
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

    def self.test_each(description, &block)
      ENDPOINT_DATABASES.each do |database_server, settings|
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
        end if ENV['ENDPOINT_DATABASES'].nil? || ENV['ENDPOINT_DATABASES'].include?(database_server)
      end
    end
  end
end
