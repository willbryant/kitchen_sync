module TestTableSchemas
  def create_footbl
    execute(<<-SQL)
      CREATE TABLE footbl (
        col1 INT NOT NULL,
        another_col INT,
        col3 VARCHAR(10),
        PRIMARY KEY(col1))
SQL
  end

  def footbl_def
    { "name"    => "footbl",
      "columns" => [
        {"name" => "col1"},
        {"name" => "another_col"},
        {"name" => "col3"}],
      "primary_key_columns" => [0] }
  end

  def create_secondtbl
    execute(<<-SQL)
      CREATE TABLE secondtbl (
        pri1 INT NOT NULL,
        pri2 CHAR(2) NOT NULL,
        sec INT,
        tri INT,
        PRIMARY KEY(pri2, pri1))
SQL
    execute(<<-SQL)
      CREATE INDEX secidx ON secondtbl (sec)
SQL
  end

  def secondtbl_def
    { "name"    => "secondtbl",
      "columns" => [
       {"name" => "pri1"},
       {"name" => "pri2"},
       {"name" => "sec"},
       {"name" => "tri"}],
      "primary_key_columns" => [1, 0] } # note order is that listed in the key, not the index of the column in the table
  end

  def create_middletbl
    execute(<<-SQL)
      CREATE TABLE middletbl (
        pri INT NOT NULL,
        PRIMARY KEY(pri))
SQL
  end

  def middletbl_def
    { "name"    => "middletbl",
      "columns" => [
        {"name" => "pri"}],
      "primary_key_columns" => [0] }
  end

  def create_texttbl
    execute(<<-SQL)
      CREATE TABLE texttbl (
        pri INT NOT NULL,
        textfield TEXT#{'(268435456)' if @database_server == 'mysql'},
        PRIMARY KEY(pri))
SQL
  end

  def texttbl_def
    { "name"    => "texttbl",
      "columns" => [
        {"name" => "pri"},
        {"name" => "textfield"}],
      "primary_key_columns" => [0] }
  end

  def create_some_tables
    clear_schema
    create_footbl
    create_secondtbl
  end

  def some_table_defs
    [footbl_def, secondtbl_def]
  end
end
