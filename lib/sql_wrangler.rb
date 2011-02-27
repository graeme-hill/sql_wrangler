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
    
    def initialize(db, sql_string)
      @db = db
      @sql_string = sql_string
    end
    
    def get_raw_result
      @db.execute(@sql_string)
    end
    
    def execute
      result = @db.execute2(@sql_string)
      anon_guid_str = Guid.new.to_s.gsub("-", "")
      anon_type = Object.const_set("Anon#{anon_guid_str}".to_sym, Class.new)
      result[0].each do |column|
        anon_type.class_eval do
          attr_accessor column.to_sym
        end
      end
      col_range = 0..(result[0].length-1)
      result[1,result.length-1].each do |row|
        instance = anon_type.new
        for i in col_range do
          column = result[0][i]
          instance.send "#{column}=", row[i]
        end
      end
    end
    
  end
  
end