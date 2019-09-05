require 'mysql2'

ENDPOINT_DATABASES["mysql"] = {
  :connect => lambda { |host, port, name, username, password| Mysql2::Client.new(
    :host     => host,
    :port     => port.to_i,
    :database => name,
    :username => username,
    :password => password,
    :cast_booleans => true)
  }
}

class Mysql2::Client
  def execute(sql)
    query(sql)
  end

  def server_version
    @server_version ||= query("SHOW VARIABLES LIKE 'version'").first["Value"]
  end

  def tables
    query("SHOW FULL TABLES").select {|row| row["Table_type"] == "BASE TABLE"}.collect {|row| row.values.first}
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

  def zero_time_value
    Time.new(2000, 1, 1, 0, 0, 0)
  end

  def supports_multiple_timestamp_columns?
    server_version !~ /^5\.5/
  end

  def mysql_default_expressions?
    # mysql 8.0+ or mariadb 10.2+ (note mariadb skipped 6 through 9)
    server_version !~ /^5\./ && server_version !~ /^10\.0.*MariaDB/ && server_version !~ /^10\.1.*MariaDB/
  end

  def default_expressions?
    mysql_default_expressions?
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

  def explicit_json_column_type?
    # supported by mysql 5.7.8+, not by mysql 5.x or mariadb
    server_version !~ /^5\.5/ && server_version !~ /MariaDB/
  end

  def supports_fractional_seconds?
    # supported by mysql 8+ or any version of mariadb, but since mariadb 5.x doesn't support precision arguments
    # to functions like CURRENT_TIMESTAMP, we only support fractional precision on non-5.x versions of either
    server_version !~ /^5\./
  end

  def supports_generated_as_identity?
    false
  end

  def sequence_column_type
    'INT NOT NULL AUTO_INCREMENT'
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

  def json_column_type(column_name)
    if explicit_json_column_type?
      "JSON"
    else
      "LONGTEXT COMMENT 'JSON' CHECK (json_valid(#{column_name}))"
    end
  end

  def jsonb_column_type?
    false
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

  def real_column_type
    'FLOAT'
  end

  def create_enum_column_type
  end

  def enum_column_type
    "ENUM('red', 'green', 'blue', 'with''quote')"
  end

  def enum_column_type_restriction
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

  def unsupported_column_type
    'bit(8)'
  end
end
