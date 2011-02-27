require 'rubygems'
require 'guid'
require 'sqlite3'

module SqlWrangler
  
  class SqlConnection
  end
  
  class SqLiteConnection < SqlConnection
    
    def initialize(db_path)
      @db = SQLite3::Database.new db_path
    end
    
    def execute_sql(sql_command)
      @db.execute(sql_command)
    end
    
    def execute_query(query)
      raw_result = @db.execute2(query.sql_string)
      formatted_result = []
      col_range = 0..(raw_result[0].length-1)
      raw_result[1,raw_result.length-1].each do |raw_row|
        formatted_row = {}
        for i in col_range do
          formatted_row[raw_result[0][i]] = raw_row[i]
        end
        formatted_result << formatted_row
      end
      return formatted_result
    end

    def query(sql_string)
      Query.new self, sql_string
    end

    def close
      @db.close
    end
    
  end
  
  class Query
    
    attr_reader :sql_string
    attr_reader :groupings
    attr_reader :conn
    
    def initialize(conn, sql_string)
      @conn = conn
      @sql_string = sql_string
      @groupings = []
    end
    
    def execute
      @conn.execute_query(self)
    end
    
    def group(name, columns)
      @groupings << QueryGrouping.new(name, columns)
      return self
    end
    
  end
  
  class QueryGrouping
    
    attr_reader :name
    attr_reader :columns
    
    def initialize(name, columns)
      @name = name
      @columns = columns
    end
    
  end
  
end