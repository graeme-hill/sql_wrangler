require 'fas_test'
require './lib/sql_wrangler'

class SqLiteConnectionTests < FasTest::TestClass
  
  def class_setup
    @conn = SqlWrangler::SqLiteConnection.new ":memory:"
  end
  
  def class_teardown
    @conn.close
  end
  
  def test__command
    begin
      @conn.command("CREATE TABLE users (id int PRIMARY KEY, username VARCHAR(100), password VARCHAR(100))")
      @conn.command("INSERT INTO users VALUES (1, 'username1', 'password1')")
      result = @conn.execute_sql("SELECT * FROM users")
      assert_equal(2, result.length)
    ensure
      @conn.command("DROP TABLE users")
    end
  end
  
  def test__query__without_executing_it
    query = @conn.query("SELECT * FROM users")
    assert_equal("SELECT * FROM users", query.sql_string)
  end
  
end

class QueryTests < FasTest::TestClass
  
  def class_setup
    @conn = SqlWrangler::SqLiteConnection.new ":memory:"
    @conn.execute_sql("CREATE TABLE users (username VARCHAR(100), password VARCHAR(100))")
    @conn.execute_sql("CREATE TABLE groups (group_name VARCHAR(100))")
    @conn.execute_sql("CREATE TABLE users_groups (username VARCHAR(100), group_name VARCHAR(100))")
    
    @conn.execute_sql("create table articles (id int primary key, title varchar(100), content varchar(100), author varchar(100))")
    @conn.execute_sql("create table comments (id int primary key, content text, article_id int)")
  end
  
  def test_setup
    @conn.execute_sql("INSERT INTO users VALUES ('username1', 'password1');")
    @conn.execute_sql("INSERT INTO users VALUES ('username2', 'password2');")
    @conn.execute_sql("INSERT INTO groups VALUES ('group one')")
    @conn.execute_sql("INSERT INTO groups VALUES ('group two')")
    @conn.execute_sql("INSERT INTO users_groups VALUES ('username1', 'group one')")
    @conn.execute_sql("INSERT INTO users_groups VALUES ('username1', 'group two')")
    @conn.execute_sql("INSERT INTO users_groups VALUES ('username2', 'group one')")
    
    @conn.execute_sql("insert into articles values (1, 'article1', 'content1', 'username1')")
    @conn.execute_sql("insert into comments values (1, 'comment on article1 #1', 1)")
    @conn.execute_sql("insert into comments values (2, 'comment on article1 #2', 1)")
  end

  def test_teardown
    @conn.execute_sql("delete from users")
    @conn.execute_sql("delete from groups")
    @conn.execute_sql("delete from users_groups")
    @conn.execute_sql("delete from articles")
    @conn.execute_sql("delete from comments")
  end
  
  def class_teardown
    @conn.close
  end
  
  def test__execute__has_correct_values_on_simple_query
    result = @conn.query("SELECT * FROM users").execute
    assert_equal(2, result.length)
    assert_equal("username1", result[0]['username'])
    assert_equal("password1", result[0]['password'])
    assert_equal("username2", result[1]['username'])
    assert_equal("password2", result[1]['password'])
  end
  
  def test__execute__has_correct_columns_on_simple_query
    first = @conn.query("SELECT * FROM users").execute[0]
    assert_true(first.keys.any? { |m| m == 'username' })
    assert_true(first.keys.any? { |m| m == 'password' })
  end
  
  def test__execute__works_with_a_more_complex_query
    result = @conn.query("
      select u.username, g.group_name 
      from groups g
      inner join users_groups ug on ug.group_name = g.group_name
      inner join users u on u.username = ug.username
      order by u.username, g.group_name").execute
    assert_equal(3, result.length)
    assert_equal("username1", result[0]["username"])
    assert_equal("group one", result[0]["group_name"])
    assert_equal("username1", result[1]["username"])
    assert_equal("group two", result[1]["group_name"])
    assert_equal("username2", result[2]["username"])
    assert_equal("group one", result[2]["group_name"])
  end
  
  def test__execute__works_with_a_simple_grouping
    result = @conn.query("
      select u.username, g.group_name 
      from groups g
      inner join users_groups ug on ug.group_name = g.group_name
      inner join users u on u.username = ug.username
      order by u.username, g.group_name").group("users", ["group_name"]).execute
    assert_equal(2, result.length)
    assert_equal("group one", result[0]["group_name"])
    assert_equal(2, result[0]["users"].length)
    assert_equal("username1", result[0]["users"][0]["username"])
    assert_equal("username2", result[0]["users"][1]["username"])
    assert_equal("group two", result[1]["group_name"])
    assert_equal(1, result[1]["users"].length)
    assert_equal("username1", result[1]["users"][0]["username"])
  end
  
  def test__group__modifies_query_object_correctly_with_single_grouping
    query = @conn.query("
      select u.username, g.group_name 
      from groups g
      inner join users_groups ug on ug.group_name = g.group_name
      inner join users u on u.username = ug.username
      order by u.username, g.group_name").group("users", ["group_name"])
    assert_equal(1, query.groupings.count)
    assert_equal("users", query.groupings[0].name)
    assert_equal(1, query.groupings[0].columns.length)
    assert_equal("group_name", query.groupings[0].columns[0])
  end
  
  def test__execute__works_with_multi_level_grouping
    result = @conn.query("
      select u.username, g.group_name, a.title, a.content article_content, c.id comment_id, c.content comment_content
      from groups g
      join users_groups ug on ug.group_name = g.group_name
      join users u on u.username = ug.username
      left join articles a on a.author = u.username
      left join comments c on c.article_id = a.id
      order by g.group_name, u.username, a.title, c.content") \
      .group("users", ["group_name"]) \
      .group("articles", ["username"]) \
      .group("comments", ["comment_id"]) \
      .execute
    assert_equal(2, result.length)
    assert_equal("group one", result[0]["group_name"])
    assert_equal(2, result[0]["users"].length)
    assert_equal("username1", result[0]["users"][0]["username"])
    
    assert_equal(1, result[0]["users"][0]["articles"].length)
    assert_equal("article1", result[0]["users"][0]["articles"][0]["title"])
    assert_equal("content1", result[0]["users"][0]["articles"][0]["article_content"])
    
    assert_equal(2, result[0]["users"][0]["articles"][0]["comments"].length)
    assert_equal(1, result[0]["users"][0]["articles"][0]["comments"][0]["comment_id"])
    assert_equal("comment on article1 #1", result[0]["users"][0]["articles"][0]["comments"][0]["comment_content"])
    assert_equal(2, result[0]["users"][0]["articles"][0]["comments"][1]["comment_id"])
    assert_equal("comment on article1 #2", result[0]["users"][0]["articles"][0]["comments"][1]["comment_content"])
    assert_equal("username2", result[0]["users"][1]["username"])
    assert_equal(0, result[0]["users"][1]["articles"].length)
    
    assert_equal("group two", result[1]["group_name"])
    assert_equal(1, result[1]["users"].length)
    assert_equal("username1", result[1]["users"][0]["username"])
    assert_equal(1, result[1]["users"][0]["articles"].length)
  end
  
end