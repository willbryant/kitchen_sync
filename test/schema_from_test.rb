require File.expand_path(File.join(File.dirname(__FILE__), 'test_helper'))

class SchemaFromTest < KitchenSync::EndpointTestCase
  def from_or_to
    :from
  end

  test_each "returns an empty list of tables on an empty database" do
    clear_schema

    assert_equal({"tables" => []}, send_command("schema"))
  end

  test_each "returns the name of the tables and the names of their columns" do
    clear_schema
    execute(<<-SQL)
      CREATE TABLE footbl (
        col1 INT NOT NULL,
        another_col INT,
        col3 CHAR(10),
        PRIMARY KEY(col1))
SQL
    execute(<<-SQL)
      CREATE TABLE secondtbl (
        pri INT NOT NULL,
        sec INT,
        tri INT,
        PRIMARY KEY(pri))
SQL
    execute(<<-SQL)
      CREATE INDEX secidx ON secondtbl (sec)
SQL

    assert_equal(
      {"tables" => [
        {"name"    => "footbl",
         "columns" => [
          {"name" => "col1"},
          {"name" => "another_col"},
          {"name" => "col3"}]},
        {"name"    => "secondtbl",
         "columns" => [
          {"name" => "pri"},
          {"name" => "sec"},
          {"name" => "tri"}]}]},
      send_command("schema"))
  end
end
