require_relative('loader')

module Wukong
  module Load

    # Loads data into MongoDB
    class MongoDBLoader < Loader

      field :host,             String, :default => 'localhost'
      field :port,             Integer,:default => 9200
      field :database,         String, :default => 'wukong'
      field :collection,       String, :default => 'streaming_record'
      field :database_field,   String, :default => '_database'
      field :collection_field, String, :default => '_collection'
      field :id_field,         String, :default => '_id'

      # The Mongo::MongoClient we'll use for talking to MongoDB.
      attr_accessor :client

      # Creates the client connection.
      def setup
        require 'mongo'
        h = host.gsub(%r{^http://},'')
        log.debug("Connecting to MongoDB server at #{h}:#{port}...")
        begin
          self.client = Mongo::MongoClient.new(h, port)
        rescue => e
          raise Error.new(e.message)
        end
      end

      # Load a single record into MongoDB.
      #
      # If the record has an ID, we'll issue an update, otherwise an
      # insert.
      #
      # @param [record] Hash
      def load record
        id = id_for(record)
        if id
          collection_for(record).update({:id => id}, record, :upsert => true)
        else
          collection_for(record).insert(record)
        end
      end

      # :nodoc:
      def database_for record
        client.database(database_name_for(record))
      end

      # :nodoc:
      def collection_for record
        database_for(record).collection(collection_name_for(record))
      end

      # :nodoc:
      def database_name_for record
        record[database_field] || self.database
      end

      # :nodoc:
      def collection_name_for record
        record[collection_field] || self.collection
      end

      # :nodoc:
      def id_for record
        record[id_field]
      end
      
      register :mongodb_loader
      
    end
  end
end

    
    
