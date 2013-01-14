require_relative('../loader')

module Wukong
  module Load

    # Loads data into SQL databases.
    #
    # Uses the 'mysql' gem to connect and write data.  Yes, MySQL !=
    # SQL but we'll get there, I promise...
    #
    # Allows loading records into a given database and table.  Records
    # can have fields `_database` and `_table` which override the
    # given database and table on a per-record basis.
    #
    # Records can have an `_id` field which indicates an update, not
    # an insert.
    #
    # The names of these fields within each record (`_database`,
    # `_table`, and `_id`) can be customized.
    class SQLLoader < Loader

      field :host,             String, :default => 'localhost', :doc => "SQL host"
      field :port,             Integer,:default => 3306, :doc => "Port on SQL host"
      field :username,         String, :default => (ENV['USER'] || 'wukong'), :doc => "User to connect as"
      field :password,         String, :doc => "Password for user"
      field :database,         String, :default => 'wukong', :doc => "Default database"
      field :table,            String, :default => 'streaming_record', :doc => "Default table"
      field :database_field,   String, :default => '_database', :doc => "Name of field in each record overriding default database"
      field :table_field,      String, :default => '_table', :doc => "Name of field in each record overriding default table"
      field :id_field,         String, :default => '_id', :doc => "Name of field in each record providing ID of existing row to update"

      description <<-EOF.gsub(/^ {8}/,'')
        Loads newline-separated, JSON-formatted records over STDIN
        into MySQL using its HTTP API.

          $ cat data.json | wu-load sql

        By default, wu-load attempts to write each input record to a
        local SQL server.

        Input records will be written to a default database and table.
        Each record can have _database and _table fields to override
        this on a per-record basis.

        Records with an _id field will be trigger updates, the rest
        inserts.

        All other fields within a record are assumed to be the names
        of actual columns in the table.

        The fields used (_index, _table, and _id) can be changed:

          $ cat data.json | wu-load sql --host=10.123.123.123 --database=web_events --table=impressions --id_field=impression_id
      EOF
      
      # The Mongo::MongoClient we'll use for talking to MongoDB.
      attr_accessor :client

      # Creates the client connection.
      def setup
        require 'mysql2'
        log.debug("Connecting to SQL server at #{host}:#{port}...")
        begin
          self.client = Mysql2::Client.new(sql_params)
        rescue => e
          raise Error.new(e)
        end
      end

      # :nodoc:
      def sql_params
        {:host => host, :port => port}.tap do |params|
          params[:username] if username
          params[:password] if password
        end
      end

      # Load a single record into the database.
      #
      # If the record has an ID, we'll issue an update, otherwise an
      # insert.
      #
      # @param [record] Hash
      def load record
        id = id_for(record)
        if id
          perform_query(update_query(record))
          log.info("Updated #{id}")
        else
          perform_query(insert_query(record))
          log.info("Inserted")
        end
      end
      
      # :nodoc:
      def insert_query record
        "INSERT INTO #{database_name_for(record)}.#{table_name_for(record)} (#{fields_of(record)}) VALUES (#{values_of(record)}) ON DUPLICATE KEY UPDATE #{fields_and_values_of(record)}"
      end

      # :nodoc:
      def update_query record
        "UPDATE #{database_name_for(record)}.#{table_name_for(record)} SET #{fields_and_values_of(record)} WHERE `id`=#{id_for(record)}"
      end

      # :nodoc:
      def field_names_of record
        record.keys.reject { |key| [database_field, table_field, id_field].include?(key) }.sort
      end

      # :nodoc:
      def fields_of record
        field_names_of(record).map { |name| identifier_for(name) }.join(', ')
      end

      # :nodoc:
      def values_of record
        field_names_of(record).map { |name| value_for(record[name]) }.join(', ')
      end

      # :nodoc:
      def fields_and_values_of record
        field_names_of(record).map { |name| [identifier_for(name), value_for(record[name])].join('=') }.join(', ')
      end

      # :nodoc:
      def database_name_for record
        identifier_for(record[database_field] || self.database)
      end

      # :nodoc:
      def table_name_for record
        identifier_for(record[table_field] || self.table)
      end

      # :nodoc:
      def identifier_for thing
        '`' + client.escape(thing.to_s) + '`'
      end

      # :nodoc:
      def value_for thing
        case thing
        when Fixnum then thing
        when nil    then 'NULL'
        else
          '"' + client.escape(thing.to_s) + '"'
        end
      end
      

      # :nodoc:
      def id_for record
        value_for(record[id_field]) if record[id_field]
      end

      # :nodoc:
      def perform_query query
        client.query query
      end
      
      register :sql_loader
      
    end
  end
end
