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
    query("SHOW KEYS FROM #{table_name}").collect {|row| row["Key_name"] unless row["Key_name"] == "PRIMARY"}.compact.uniq
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
    "`#{name.gsub('`', '``')}`"
  end

  def zero_time_value
    Time.new(2000, 1, 1, 0, 0, 0)
  end
end
