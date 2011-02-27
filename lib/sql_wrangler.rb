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

    def query(sql_string)
      SqLiteQuery.new @db, sql_string
    end

    def close
      @db.close
    end
    
  end
  
  class SqLiteQuery
    
    attr_reader :sql_string
    attr_reader :db
    attr_reader :groupings
    
    def initialize(db, sql_string)
      @db = db
      @sql_string = sql_string
      @groupings = []
    end
    
    def get_raw_result
      @db.execute(@sql_string)
    end
    
    def execute
      raw_result = @db.execute2(@sql_string)
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