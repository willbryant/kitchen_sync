require 'mysql2'
require 'forwardable'

class MysqlAdapter
  def host
    ENV["MYSQL_DATABASE_HOST"]     || ENV["ENDPOINT_DATABASE_HOST"]     || ""
  end

  def port
    ENV["MYSQL_DATABASE_PORT"]     || ENV["ENDPOINT_DATABASE_PORT"]     || ""
  end

  def name
    ENV["MYSQL_DATABASE_NAME"]     || ENV["ENDPOINT_DATABASE_NAME"]     || "ks_test"
  end

  def username
    ENV["MYSQL_DATABASE_USERNAME"] || ENV["ENDPOINT_DATABASE_USERNAME"] || ""
  end

  def password
    ENV["MYSQL_DATABASE_PASSWORD"] || ENV["ENDPOINT_DATABASE_PASSWORD"] || ""
  end

  def initialize
    @connection = Mysql2::Client.new(
      :host          => host,
      :port          => port.to_i,
      :database      => name,
      :username      => username,
      :password      => password,
      :cast_booleans => true)
  end

  extend Forwardable
  def_delegators :@connection, :query, :escape, :close
  alias_method :execute, :query

  def server_version
    @server_version ||= query("SHOW VARIABLES LIKE 'version'").first["Value"]
  end

  def tables
    query("SHOW FULL TABLES").select {|row| row["Table_type"] == "BASE TABLE"}.collect {|row| row.values.first}
  end

  def clear_schema
    views.each {|view_name| execute "DROP VIEW #{quote_ident view_name}"}
    tables.each {|table_name| execute "DROP TABLE #{quote_ident table_name}"}
  end

  def views
    query("SHOW FULL TABLES").select {|row| row["Table_type"] == "VIEW"}.collect {|row| row.values.first}
  end

  def table_primary_key_name(table_name)
    "PRIMARY"
  end

  def table_keys(table_name)
    query("SHOW KEYS FROM #{quote_ident table_name}").collect {|row| row["Key_name"] unless row["Key_name"] == "PRIMARY"}.compact.uniq
  end

  def table_keys_unique(table_name)
    query("SHOW KEYS FROM #{quote_ident table_name}").each_with_object({}) {|row, results| results[row["Key_name"]] = row["Non_unique"].zero? unless row["Key_name"] == "PRIMARY"}
  end

  def table_key_columns(table_name)
    query("SHOW KEYS FROM #{quote_ident table_name}").each_with_object({}) {|row, results| (results[row["Key_name"]] ||= []) << row["Column_name"]}
  end

  def table_column_names(table_name)
    query("SHOW COLUMNS FROM #{quote_ident table_name}").collect {|row| row.values.first}.compact
  end

  def table_column_types(table_name)
    query("SHOW COLUMNS FROM #{quote_ident table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = row["Type"]}
  end

  def table_srids(table_name)
    query("SHOW CREATE TABLE #{quote_ident table_name}").first["Create Table"].scan(/`(.*)`.*\/\*!80003 SRID (\d+)/).to_h
  end

  def table_column_nullability(table_name)
    query("SHOW COLUMNS FROM #{quote_ident table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = (row["Null"] == "YES")}
  end

  def table_column_defaults(table_name)
    query("SHOW COLUMNS FROM #{quote_ident table_name}").collect.with_object({}) {|row, results| results[row["Field"]] = row["Default"]}
  end

  def quote_ident(name)
    "`#{name.gsub('`', '``')}`"
  end

  def supports_multiple_schemas?
    false
  end

  def zero_time_value
    Time.new(2000, 1, 1, 0, 0, 0)
  end

  def supports_multiple_timestamp_columns?
    server_version !~ /^5\.5/
  end

  def default_expressions?
    # mysql 8.0+ or mariadb 10.2+ (note mariadb skipped 6 through 9)
    server_version !~ /^5\./ && server_version !~ /^10\.0.*MariaDB/ && server_version !~ /^10\.1.*MariaDB/
  end

  def spatial_axis_order_depends_on_srs?
    # only mysql 8+ behaves this way
    server_version !~ /^5\./ && server_version !~ /MariaDB/
  end

  def supports_spatial_indexes?
    # supported by mysql 5.7+, and mariadb 10.2+
    server_version !~ /^5\.5/ && server_version !~ /^10\.0.*MariaDB/ && server_version !~ /^10\.1.*MariaDB/
  end

  def schema_srid_settings?
    # supported by mysql 8+, not by mysql 5.x or mariadb
    server_version !~ /^5\./ && server_version !~ /MariaDB/
  end

  def supports_fractional_seconds?
    # supported by mysql 8+ or any version of mariadb, but since mariadb 5.x doesn't support precision arguments
    # to functions like CURRENT_TIMESTAMP, we only support fractional precision on non-5.x versions of either
    server_version !~ /^5\./
  end

  def supports_generated_as_identity?
    false
  end

  def supports_generated_columns?
    # in fact, generated columns are supported by mysql 5.7+, and mariadb 5.2+, which covers all the versions we test against.
    # however, mariadb 5.2 through 10.1 don't show the generation expressions in SHOW COLUMNS or INFORMATION_SCHEMA.COLUMNS,
    # so we don't actually support them in KS because although we could create such columns we couldn't extract the schema
    # back again.  (they do show in SHOW CREATE TABLE, but we don't parse that, and it isn't worth the effort of converting
    # all of our schema loading code to parse SHOW CREATE TABLE for the sake of old mariadb versions.)  in addition, older
    # versions of mariadb only support VIRTUAL generated columns (interestingly, the reverse of postgresql, which currently
    # only supports STORED generated columns).
    server_version !~ /^5\./ && server_version !~ /^10\.0.*MariaDB/ && server_version !~ /^10\.1.*MariaDB/
  end

  def supports_virtual_generated_columns?
    true
  end

  def quote_ident_for_generation_expression(name)
    quote_ident name
  end

  def identity_default_type
    "generated_by_default_as_identity"
  end

  def identity_default_name(table_name, column_name)
    ""
  end

  def identity_column_type
    'INT NOT NULL AUTO_INCREMENT'
  end

  def uuid_column_type?
    false
  end

  def uuid_column_type
    "CHAR(36) COMMENT 'UUID'"
  end

  def text_column_type
    'LONGTEXT'
  end

  def blob_column_type
    'LONGBLOB'
  end

  def explicit_json_column_type?
    # supported by mysql 5.7.8+, not by mysql 5.x or mariadb
    server_version !~ /^5\.5/ && server_version !~ /MariaDB/
  end

  def json_column_type?
    # supported by mysql 5.7+ (explicit type), and mariadb 10.2+ (using check constraints)
    server_version !~ /^5\.5/ && server_version !~ /^10\.0.*MariaDB/ && server_version !~ /^10\.1.*MariaDB/
  end

  def jsonb_column_type?
    false
  end

  def json_column_type(column_name)
    if explicit_json_column_type?
      "JSON"
    else
      "LONGTEXT CHECK (json_valid(#{column_name}))"
    end
  end

  def time_column_type
    if supports_fractional_seconds?
      'TIME(6)'
    else
      'TIME'
    end
  end

  def datetime_column_type
    if supports_fractional_seconds?
      'DATETIME(6)'
    else
      'DATETIME'
    end
  end

  def time_precision
    if supports_fractional_seconds?
      {'size' => 6}
    else
      {}
    end
  end

  def time_zone_types?
    false
  end

  def real_column_type
    'FLOAT'
  end

  def create_enum_column_type
  end

  def enum_column_type
    "ENUM('red', 'green', 'blue', 'with''quote')"
  end

  def enum_column_subtype
    # mysql doesn't name the specific enumeration types
    {}
  end

  def install_spatial_support
  end

  def uninstall_spatial_support
  end

  def create_spatial_index(index_name, table_name, *columns)
    execute "CREATE SPATIAL INDEX #{quote_ident index_name} ON #{quote_ident table_name} (#{columns.join ', '})"
  end

  def spatial_column_type(geometry_type: 'geometry', srid:)
    "#{geometry_type}#{" SRID #{srid}" if srid}"
  end

  def spatial_reference_table_definitions
    []
  end

  def unsupported_column_type_name
    ColumnType::MYSQL_SPECIFIC
  end

  def unsupported_sql_type
    'bit(8)'
  end

  def unsupported_column_value
    "A" # you can use the nicer b'...' syntax, but it just comes back as raw binary data like below anyway, so it doesn't really matter
  end

  def unsupported_column_value_returned
    "A"
  end

  def create_adapterspecifictbl
    execute(<<-SQL)
      CREATE TABLE ```mysql``tbl` (
        pri INT UNSIGNED NOT NULL AUTO_INCREMENT,
        parent_id INT UNSIGNED,
        tiny2 TINYINT(2) UNSIGNED DEFAULT 99,
        nulldefaultstr VARCHAR(255) DEFAULT NULL,
        secondstime TIME,
        #{"tenthstime TIME(1)," if supports_fractional_seconds?}
        #{"millistime TIME(3)," if supports_fractional_seconds?}
        #{"microstime TIME(6)," if supports_fractional_seconds?}
        secondsdatetime DATETIME,
        #{"tenthsdatetime DATETIME(1)," if supports_fractional_seconds?}
        #{"millisdatetime DATETIME(3)," if supports_fractional_seconds?}
        #{"microsdatetime DATETIME(6)," if supports_fractional_seconds?}
        timestampboth TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        #{"timestampcreateonly TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," if supports_multiple_timestamp_columns?}
        #{"microstimestampboth TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)," if supports_fractional_seconds?}
        #{"mysqlfunctiondefault VARCHAR(255) DEFAULT (uuid())," if default_expressions?}
        varbinary3 VARBINARY(3),
        fixedbinary3 BINARY(3),
        `select` INT,
        ```quoted``` INT,
        dbllen DOUBLE(12, 4),
        PRIMARY KEY(pri),
        FOREIGN KEY (parent_id) REFERENCES ```mysql``tbl`(pri) ON DELETE RESTRICT)
SQL
  end

  def adapterspecifictbl_def(compatible_with: self, schema_name: nil)
    { "name"    => "`mysql`tbl",
      "columns" => [
        {"name" => "pri",                   "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::UINT_32BIT : ColumnType::SINT_32BIT, "nullable" => false, identity_default_type => ""},
        {"name" => "parent_id",             "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::UINT_32BIT : ColumnType::SINT_32BIT},
        {"name" => "tiny2",                 "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::UINT_8BIT : ColumnType::SINT_16BIT, "default_value" => "99"}, # note we've lost the (nonportable) display width (2) - size tells us the size of the integers, not the display width
        {"name" => "nulldefaultstr",        "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255},
        {"name" => "secondstime",           "column_type" => ColumnType::TIME},
        ({"name" => "tenthstime",           "column_type" => ColumnType::TIME, "size" => 1} if supports_fractional_seconds?),
        ({"name" => "millistime",           "column_type" => ColumnType::TIME, "size" => 3} if supports_fractional_seconds?),
        ({"name" => "microstime",           "column_type" => ColumnType::TIME, "size" => 6} if supports_fractional_seconds?),
        {"name" => "secondsdatetime",       "column_type" => ColumnType::DATETIME},
        ({"name" => "tenthsdatetime",       "column_type" => ColumnType::DATETIME, "size" => 1} if supports_fractional_seconds?),
        ({"name" => "millisdatetime",       "column_type" => ColumnType::DATETIME, "size" => 3} if supports_fractional_seconds?),
        ({"name" => "microsdatetime",       "column_type" => ColumnType::DATETIME, "size" => 6} if supports_fractional_seconds?),
        {"name" => "timestampboth",         "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::DATETIME_MYSQLTIMESTAMP : ColumnType::DATETIME,               "nullable" => false, "default_expression" => "CURRENT_TIMESTAMP", "auto_update" => "current_timestamp"},
        ({"name" => "timestampcreateonly",  "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::DATETIME_MYSQLTIMESTAMP : ColumnType::DATETIME,               "nullable" => false, "default_expression" => "CURRENT_TIMESTAMP"} if supports_multiple_timestamp_columns?),
        ({"name" => "microstimestampboth",  "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::DATETIME_MYSQLTIMESTAMP : ColumnType::DATETIME, "size" => 6,  "nullable" => false, "default_expression" => "CURRENT_TIMESTAMP(6)", "auto_update" => "current_timestamp"} if supports_fractional_seconds?),
        ({"name" => "mysqlfunctiondefault", "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255,                                "default_expression" => "uuid()"} if default_expressions?),
        {"name" => "varbinary3",            "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::BINARY_VARBINARY : ColumnType::BINARY, "size" => 3},
        {"name" => "fixedbinary3",          "column_type" => compatible_with.is_a?(MysqlAdapter) ? ColumnType::BINARY_FIXED     : ColumnType::BINARY, "size" => 3},
        {"name" => "select",                "column_type" => ColumnType::SINT_32BIT},
        {"name" => "`quoted`",              "column_type" => ColumnType::SINT_32BIT},
        {"name" => "dbllen",                "column_type" => ColumnType::FLOAT_64BIT, "size" => 12, "scale" => 4},
      ].compact,
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [{"name" => "parent_id", "columns" => [1]}] }.merge( # automatically created
      schema_name ? { "schema_name" => schema_name } : {}) # not actually supported by mysql, but error handling is tested
  end

  def adapterspecifictbl_row
    {
      "tiny2" => 12,
      "timestampboth" => "2019-07-03 00:00:01",
      "varbinary3" => "\xff\x00".force_encoding("ASCII-8BIT"),
      "fixedbinary3" => "\xff\x00\x01".force_encoding("ASCII-8BIT"),
    }.merge(
      supports_fractional_seconds? ? { "microstimestampboth" => "2019-07-03 01:02:03.123456" } : {})
  end

  def supported_column_types
    Set.new([
      ColumnType::UNKNOWN,
      ColumnType::MYSQL_SPECIFIC,
      ColumnType::BINARY,
      ColumnType::BINARY_VARBINARY,
      ColumnType::BINARY_FIXED,
      ColumnType::TEXT,
      ColumnType::TEXT_VARCHAR,
      ColumnType::TEXT_FIXED,
      ColumnType::BOOLEAN,
      ColumnType::SINT_8BIT,
      ColumnType::SINT_16BIT,
      ColumnType::SINT_24BIT,
      ColumnType::SINT_32BIT,
      ColumnType::SINT_64BIT,
      ColumnType::UINT_8BIT,
      ColumnType::UINT_16BIT,
      ColumnType::UINT_24BIT,
      ColumnType::UINT_32BIT,
      ColumnType::UINT_64BIT,
      ColumnType::FLOAT_64BIT,
      ColumnType::FLOAT_32BIT,
      ColumnType::DECIMAL,
      ColumnType::DATE,
      ColumnType::TIME,
      ColumnType::DATETIME,
      ColumnType::DATETIME_MYSQLTIMESTAMP,
      ColumnType::SPATIAL,
      ColumnType::ENUMERATION,
    ]).tap do |results|
      results << ColumnType::JSON if json_column_type?
      results << ColumnType::SPATIAL_GEOGRAPHY if schema_srid_settings?
    end
  end
end

ENDPOINT_ADAPTERS["mysql"] = MysqlAdapter
