require 'pg'
require 'forwardable'

class PostgreSQLAdapter
  def host
    ENV["POSTGRESQL_DATABASE_HOST"]     || ENV["ENDPOINT_DATABASE_HOST"]     || ""
  end

  def port
    ENV["POSTGRESQL_DATABASE_PORT"]     || ENV["ENDPOINT_DATABASE_PORT"]     || ""
  end

  def name
    ENV["POSTGRESQL_DATABASE_NAME"]     || ENV["ENDPOINT_DATABASE_NAME"]     || "ks_test"
  end

  def username
    ENV["POSTGRESQL_DATABASE_USERNAME"] || ENV["ENDPOINT_DATABASE_USERNAME"] || ""
  end

  def password
    ENV["POSTGRESQL_DATABASE_PASSWORD"] || ENV["ENDPOINT_DATABASE_PASSWORD"] || ""
  end

  def initialize
    PG::BasicTypeRegistry.alias_type(0, 'time', 'text')
    PG::BasicTypeRegistry.alias_type(0, 'uuid', 'text')
    PG::BasicTypeRegistry.alias_type(0, 'json', 'text') # disable the default conversion of JSON fields to Ruby hashes, as we treat them as strings in KS, so it would make the tests awkward to write if they came back as hashes
    @connection = PG.connect(
      host,
      port,
      nil,
      nil,
      name,
      username,
      password
    )
    @connection.type_map_for_results = PG::BasicTypeMapForResults.new(@connection)
  end

  extend Forwardable
  def_delegators :@connection, :query, :server_version, :escape

  def execute(sql)
    @connection.async_exec(sql)
  end

  def tables
    query("SELECT tablename::TEXT FROM pg_tables WHERE schemaname = ANY (current_schemas(false)) ORDER BY tablename").collect {|row| row["tablename"]}
  end

  def views
    query("SELECT viewname::TEXT FROM pg_views WHERE schemaname = ANY (current_schemas(false)) ORDER BY viewname").collect {|row| row["viewname"]}
  end

  def sequence_generators
    # from pg 10 on we would be able to be more consistent with the above: query("SELECT sequencename::TEXT FROM pg_sequences WHERE schemaname = ANY (current_schemas(false)) ORDER BY sequencename").collect {|row| row["sequencename"]}
    query("SELECT sequence_name::TEXT FROM INFORMATION_SCHEMA.SEQUENCES WHERE sequence_schema = ANY (current_schemas(false)) ORDER BY sequence_name").collect {|row| row["sequence_name"]}
  end

  def table_primary_key_name(table_name)
    "#{table_name}_pkey"
  end

  def table_keys(table_name)
    query(<<-SQL).collect {|row| row["relname"]}
      SELECT index_class.relname::TEXT
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
      SELECT index_class.relname::TEXT, indisunique
        FROM pg_class table_class, pg_index, pg_class index_class
       WHERE table_class.relname = '#{table_name}' AND
             table_class.oid = pg_index.indrelid AND
             index_class.oid = pg_index.indexrelid AND
             index_class.relkind = 'i' AND
             NOT pg_index.indisprimary
    SQL
  end

  def key_definition_columns(definition)
    if definition =~ /\((.*)\)$/
      $1.split(', ')
    end
  end

  def table_key_columns(table_name)
    query(<<-SQL).each_with_object({}) {|row, results| results[row["relname"]] = key_definition_columns(row["definition"])}
      SELECT index_class.relname::TEXT, pg_get_indexdef(indexrelid) AS definition
        FROM pg_class table_class, pg_class index_class, pg_index
       WHERE table_class.relname = '#{table_name}' AND
             table_class.relkind = 'r' AND
             index_class.relkind = 'i' AND
             pg_index.indrelid = table_class.oid AND
             pg_index.indexrelid = index_class.oid
       ORDER BY relname
    SQL
  end

  def table_column_names(table_name)
    query(<<-SQL).collect {|row| row["attname"]}
      SELECT attname::TEXT
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
      SELECT attname::TEXT, format_type(atttypid, atttypmod) AS atttype
        FROM pg_attribute, pg_class, pg_type
       WHERE attrelid = pg_class.oid AND
             atttypid = pg_type.oid AND
             attnum > 0 AND
             NOT attisdropped AND
             relname = '#{table_name}'
       ORDER BY attnum
    SQL
  end

  def table_srids(table_name)
    table_column_types(table_name).each_with_object({}) {|(column_name, type), results| results[column_name] = Array(type.scan(/(?:geometry|geography)\(\w+,(\d+)\)/)[0])[0]}
  end

  def table_column_nullability(table_name)
    query(<<-SQL).collect.with_object({}) {|row, results| results[row["attname"]] = !row["attnotnull"]}
      SELECT attname::TEXT, attnotnull
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
      SELECT attname::TEXT, (CASE WHEN atthasdef THEN pg_get_expr(adbin, adrelid) ELSE NULL END) AS attdefault
        FROM pg_attribute
        JOIN pg_class ON attrelid = pg_class.oid
        LEFT JOIN pg_attrdef ON adrelid = attrelid AND adnum = attnum
       WHERE attnum > 0 AND
             NOT attisdropped AND
             relname = '#{table_name}'
       ORDER BY attnum
    SQL
  end

  def quote_ident(name)
    PG::Connection.quote_ident(name)
  end

  def quote_ident_for_generation_expression(name)
    # postgresql doesn't quote these all the time, unlike mysql
    name
  end

  def zero_time_value
    "00:00:00"
  end

  def supports_multiple_timestamp_columns?
    true
  end

  def spatial_axis_order_depends_on_srs?
    false
  end

  def supports_spatial_indexes?
    true
  end

  def schema_srid_settings?
    true
  end

  def supports_generated_as_identity?
    server_version >= 100000
  end

  def supports_generated_columns?
    server_version >= 120000
  end

  def supports_virtual_generated_columns?
    false
  end

  def identity_default_type
    if supports_generated_as_identity?
      "generated_by_default_as_identity"
    else
      "generated_by_sequence"
    end
  end

  def identity_default_name(table_name, column_name)
    if supports_generated_as_identity?
      ""
    else
      "#{table_name}_#{column_name}_seq"
    end
  end

  def identity_column_type
    if supports_generated_as_identity?
      'integer GENERATED BY DEFAULT AS IDENTITY'
    else
      'SERIAL'
    end
  end

  def uuid_column_type?
    true
  end

  def uuid_column_type
    'uuid'
  end

  def text_column_type
    'text'
  end

  def blob_column_type
    'bytea'
  end

  def json_column_type?
    true
  end

  def jsonb_column_type?
    server_version >= 90400
  end

  def json_column_type(fieldname)
    'json'
  end

  def time_column_type
    'time'
  end

  def datetime_column_type
    'timestamp'
  end

  def time_precision
    {'size' => 6}
  end

  def supports_fractional_seconds?
    true
  end

  def time_zone_types?
    true
  end

  def real_column_type
    'real'
  end

  def create_enum_column_type
    execute "DROP TYPE IF EXISTS #{enum_column_type}"
    execute "CREATE TYPE #{enum_column_type} AS ENUM('red', 'green', 'blue', 'with''quote')"
  end

  def enum_column_type
    'our_enum_t'
  end

  def enum_column_subtype
    {'subtype' => enum_column_type}
  end

  def install_spatial_support
    raise Test::Unit::OmittedError.new("Skipping test that requires PostGIS") if ENV['SKIP_POSTGIS']
    @spatial_support = true
    execute "CREATE EXTENSION postgis"
  end

  def uninstall_spatial_support
    @spatial_support = false
    execute "DROP EXTENSION IF EXISTS postgis"
  end

  def spatial_support?
    @spatial_support
  end

  def create_spatial_index(index_name, table_name, *columns)
    execute "CREATE INDEX #{index_name} ON #{table_name} USING gist (#{columns.join ', '})"
  end

  def spatial_column_type(geometry_type: 'geometry', srid:)
    srid ? "geography(#{geometry_type},#{srid})" : "geometry(#{geometry_type})"
  end

  def spatial_reference_table_definitions
    [
      # created by postgis in the public schema, and a PITA to move anywhere else
      { "name" => "spatial_ref_sys",
        "columns" => [
          {"name" => "srid",      "column_type" => ColumnType::SINT_32BIT, "nullable" => false},
          {"name" => "auth_name", "column_type" => ColumnType::TEXT_VARCHAR, "size" => 256},
          {"name" => "auth_srid", "column_type" => ColumnType::SINT_32BIT},
          {"name" => "srtext",    "column_type" => ColumnType::TEXT_VARCHAR, "size" => 2048},
          {"name" => "proj4text", "column_type" => ColumnType::TEXT_VARCHAR, "size" => 2048}],
        "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
        "primary_key_columns" => [0],
        "keys" => [] },
    ]
  end

  def unsupported_column_type_name
    ColumnType::POSTGRESQL_SPECIFIC
  end

  def unsupported_sql_type
    'text[]'
  end

  def unsupported_column_value
    "{isn't,\"a scalar\"}"
  end

  def unsupported_column_value_returned
    ["isn't", "a scalar"]
  end

  def create_adapterspecifictbl
    execute("CREATE SEQUENCE second_seq")
    execute(<<-SQL)
      CREATE TABLE """postgresql""tbl" (
        pri #{supports_generated_as_identity? ? 'integer GENERATED ALWAYS AS IDENTITY' : 'SERIAL'},
        second bigint NOT NULL DEFAULT nextval('second_seq'::regclass),
        parent_id int REFERENCES """postgresql""tbl" (pri),
        uuidfield uuid,
        jsonfield json,
        jsonbfield #{jsonb_column_type? ? "jsonb" : "json"},
        nolengthvaryingfield CHARACTER VARYING,
        noprecisionnumericfield NUMERIC,
        nulldefaultstr VARCHAR(255) DEFAULT NULL,
        currentdatefield DATE DEFAULT CURRENT_DATE,
        currentuserdefault VARCHAR(255) DEFAULT current_user,
        pgfunctiondefault TEXT DEFAULT version(),
        timewithzone time with time zone,
        timestampwithzone timestamp with time zone,
        "select" INT,
        "\"\"quoted\"\"" INT,
        PRIMARY KEY(pri))
SQL
  end

  def adapterspecifictbl_def(compatible_with: self)
    { "name"    => '"postgresql"tbl',
      "columns" => [
        {"name" => "pri",                     "column_type" => ColumnType::SINT_32BIT, "nullable" => false}.merge(supports_generated_as_identity? ? {"generated_always_as_identity" => ""} : {"generated_by_sequence" => identity_default_name('"postgresql"tbl', 'pri')}),
        {"name" => "second",                  "column_type" => ColumnType::SINT_64BIT, "nullable" => false, "generated_by_sequence" => "second_seq"},
        {"name" => "parent_id",               "column_type" => ColumnType::SINT_32BIT},
        {"name" => "uuidfield"}.merge(compatible_with.uuid_column_type? ? {"column_type" => ColumnType::UUID} : {"column_type" => ColumnType::TEXT_FIXED, "size" => 36}),
        {"name" => "jsonfield",               "column_type" => compatible_with.json_column_type? ? ColumnType::JSON : ColumnType::TEXT},
        {"name" => "jsonbfield",              "column_type" => compatible_with.jsonb_column_type? ? ColumnType::JSON_BINARY : (compatible_with.json_column_type? ? ColumnType::JSON : ColumnType::TEXT)},
        {"name" => "nolengthvaryingfield",    "column_type" => ColumnType::TEXT_VARCHAR},
        {"name" => "noprecisionnumericfield", "column_type" => ColumnType::DECIMAL},
        {"name" => "nulldefaultstr",          "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255, "default_expression" => "NULL"}, # note different to mysql, where no default and DEFAULT NULL are the same thing
        {"name" => "currentdatefield",        "column_type" => ColumnType::DATE,                        "default_expression" => CaseInsensitiveString.new("CURRENT_DATE")},
        {"name" => "currentuserdefault",      "column_type" => ColumnType::TEXT_VARCHAR, "size" => 255, "default_expression" => CaseInsensitiveString.new("CURRENT_USER")},
        {"name" => "pgfunctiondefault",       "column_type" => ColumnType::TEXT,                        "default_expression" => "version()"},
        {"name" => "timewithzone",            "column_type" => compatible_with.time_zone_types? ? ColumnType::TIME_TZ     : ColumnType::TIME,     "size" => 6},
        {"name" => "timestampwithzone",       "column_type" => compatible_with.time_zone_types? ? ColumnType::DATETIME_TZ : ColumnType::DATETIME, "size" => 6},
        {"name" => "select",                  "column_type" => ColumnType::SINT_32BIT},
        {"name" => "\"quoted\"",              "column_type" => ColumnType::SINT_32BIT},
      ].compact,
      "primary_key_type" => PrimaryKeyType::EXPLICIT_PRIMARY_KEY,
      "primary_key_columns" => [0],
      "keys" => [] }
  end

  def adapterspecifictbl_row
    { "uuidfield" => "3d190b75-dbb1-4d34-a41e-d590c1c8a895",
      "nolengthvaryingfield" => "test data",
      "noprecisionnumericfield" => "1234567890.0987654321" }
  end

  def supported_column_types
    Set.new([
      ColumnType::UNKNOWN,
      ColumnType::POSTGRESQL_SPECIFIC,
      ColumnType::BINARY,
      ColumnType::TEXT,
      ColumnType::TEXT_VARCHAR,
      ColumnType::TEXT_FIXED,
      ColumnType::JSON,
      ColumnType::UUID,
      ColumnType::BOOLEAN,
      ColumnType::SINT_16BIT,
      ColumnType::SINT_32BIT,
      ColumnType::SINT_64BIT,
      ColumnType::FLOAT_64BIT,
      ColumnType::FLOAT_32BIT,
      ColumnType::DECIMAL,
      ColumnType::DATE,
      ColumnType::TIME,
      ColumnType::TIME_TZ,
      ColumnType::DATETIME,
      ColumnType::DATETIME_TZ,
      ColumnType::ENUMERATION,
    ]).tap do |results|
      results << ColumnType::JSON_BINARY if jsonb_column_type?
      results << ColumnType::SPATIAL if spatial_support?
      results << ColumnType::SPATIAL_GEOGRAPHY if spatial_support?
    end
  end
end

ENDPOINT_ADAPTERS["postgresql"] = PostgreSQLAdapter
