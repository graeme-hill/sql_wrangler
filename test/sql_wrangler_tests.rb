require 'rubygems'
require 'fas_test'
require 'guid'
require 'lib/sql_wrangler'

class SqLiteConnectionTests < FasTest::TestClass
  
  def class_setup
    @conn = SqlWrangler::SqLiteConnection.new ":memory:"
  end
  
  def class_teardown
    @conn.close
  end
  
  def test__command
    begin
      @conn.execute_sql("CREATE TABLE users (id VARCHAR(100) PRIMARY KEY, username VARCHAR(100), password VARCHAR(100))")
      @conn.execute_sql("INSERT INTO users VALUES ('#{Guid.new.to_s}', 'username1', 'password1')")
      result = @conn.execute_sql("SELECT * FROM users")
      assert_equal(1, result.length)
    ensure
      @conn.execute_sql("DROP TABLE users")
    end
  end
  
  def test__query__without_executing_it
    query = @conn.query("SELECT * FROM users")
    assert_equal("SELECT * FROM users", query.sql_string)
    assert_true(query.db != nil)
  end
  
end

class SqLiteQueryTests < FasTest::TestClass
  
  def class_setup
    @conn = SqlWrangler::SqLiteConnection.new ":memory:"
    @conn.execute_sql("CREATE TABLE users (username VARCHAR(100), password VARCHAR(100));")
  end
  
  def test_setup
    @conn.execute_sql("INSERT INTO users VALUES ('username1', 'password1');")
    @conn.execute_sql("INSERT INTO users VALUES ('username2', 'password2');")
  end

  def test_teardown
    @conn.execute_sql("DELETE FROM users")
  end
  
  def class_teardown
    @conn.close
  end
  
  def test__get_raw_result__returns_correct_result
    result = @conn.query("SELECT username, password FROM users").get_raw_result
    assert_equal(2, result.length)
    assert_equal("username1", result[0][0])
    assert_equal("password1", result[0][1])
    assert_equal("username2", result[1][0])
    assert_equal("password2", result[1][1])
  end
  
  def test__execute__has_correct_values_on_simple_query
    result = @conn.query("SELECT * FROM users").execute
    assert_equal(2, result.length)
    assert_equal("username1", result[0].username)
    assert_equal("password1", result[0].password)
    assert_equal("username2", result[1].username)
    assert_equal("password2", result[1].password)
  end
  
  def test__execute__has_correct_columns_on_simple_query
    first = @conn.query("SELECT * FROM users").execute[0]
    assert_true(first[0].methods.any? { |m| m == 'username' })
    assert_true(first[0].methods.any? { |m| m == 'password' })
  end
  
end