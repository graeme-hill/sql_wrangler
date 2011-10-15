require 'sqlite3'

module SqlWrangler

  class SqlConnection

    def query(sql_string)
      Query.new self, sql_string
    end

  end

  class SqLiteConnection < SqlConnection

    def initialize(db_path)
      @db = SQLite3::Database.new db_path
    end

    def execute_sql(sql_string)
      @db.execute2(sql_string)
    end

    def command(sql_string)
      @db.execute_batch(sql_string)
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
      raw_result = @conn.execute_sql(@sql_string)
      init_groups_for_execution raw_result[0]
      return format_query_result(raw_result)
    end

    def format_query_result(raw_result)
      formatted_result = []
      columns_by_index = get_columns_by_index raw_result[0]

      raw_result[1,raw_result.length-1].each do |raw_row|
        merge_row raw_row, @groupings, formatted_result, columns_by_index
      end

      return formatted_result
    end

    def get_columns_by_index columns
      columns_by_index = {}
      (0..columns.length-1).each { |i| columns_by_index[i] = columns[i] }
      return columns_by_index
    end

    def init_groups_for_execution columns
      used_indexes = []
      @groupings.each do |group|
        group.group_indexes = []
        group.content_indexes = []
        (0..(columns.length-1)).each do |i|
          if group.columns.any? { |c| c == columns[i] }
            used_indexes << i
            group.group_indexes << i
          elsif not used_indexes.any? { |used_index| used_index == i }
            group.content_indexes << i
          end
        end
      end
    end

    def merge_row(row, groups, grouped_data, columns_by_index)

      if not @groupings.any?
        flat_row = {}
        columns_by_index.each { |index, column| flat_row[column] = row[index] }
        grouped_data << flat_row
      elsif groups.any?
        this_group = groups[0]
        remaining_groups = groups[1, groups.length-1]
        grouped_vals = {}
        this_group.group_indexes.each { |index| grouped_vals[columns_by_index[index]] = row[index] }

        if grouped_vals.values.any? { |value| value != nil }
          grouped_row = get_existing_grouped_row grouped_vals, grouped_data

          if grouped_row == nil
            grouped_row = grouped_vals
            grouped_row[this_group.name] = [] if this_group != nil
            grouped_data << grouped_row
          end

          merge_row(row, remaining_groups, grouped_row[this_group.name], columns_by_index)
        end

      else
        leaf_data = {}
        @groupings.last.content_indexes.each { |i| leaf_data[columns_by_index[i]] = row[i] }
        grouped_data << leaf_data if leaf_data.values.any? { |value| value != nil }
      end

    end

    def get_existing_grouped_row grouped_vals, grouped_data
      grouped_data.each do |grouped_row|
        if not grouped_vals.keys.any? { |key| grouped_vals[key] != grouped_row[key] }
          return grouped_row
        end
      end
      return nil
    end

    def group_by columns, options = {}
      new_grouping = QueryGrouping.new(options[:into], columns)
      new_grouping.level = @groupings.length
      @groupings << new_grouping
      return self
    end

  end

  class QueryGrouping

    attr_reader :name
    attr_reader :columns
    attr_accessor :content_indexes
    attr_accessor :group_indexes
    attr_accessor :level

    def initialize(name, columns)
      @name = name
      @columns = columns
    end

  end

end
