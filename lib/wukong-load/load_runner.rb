# We have to require these, **not** autoload them because we need them
# to register themselves so that Wukong can find them later when
# asked.
require_relative 'loaders/elasticsearch_loader'
require_relative 'loaders/kafka_loader'
require_relative 'loaders/mongodb_loader'
require_relative 'loaders/sql_loader'

module Wukong
  module Load

    # Implements the wu-load command.
    class LoadRunner < Wukong::Local::LocalRunner

      include Logging
      
      usage "DATA_STORE"

      description <<-EOF.gsub(/^ {8}/,'')
wu-load is a tool for loading data from Wukong into data stores.  It
supports multiple, pluggable data stores, including:

   elasticsearch
   kafka
   mongodb
   sql
   hbase (planned)

For more help on a specific loader, run:

  $ wu-load STORE_TYPE --help
EOF

      # Ensure that we were passed a data store name that we know
      # about.
      #
      # @raise [Wukong::Error] if the data store is missing or unknown
      # @return [true]
      def validate
        case
        when data_store_name.nil?
          raise Error.new("Must provide the name of a data store as the first argument")
        when processor.nil?
          raise Error.new("No loader defined for data store <#{data_store_name}>")
        end
        true
      end
      
      # The name of the data store to load data to.
      #
      # @return [String]
      def data_store_name
        args.first
      end

      # The name of the processor that should 
      #
      # @return [String]
      def dataflow
        case data_store_name
        when 'elasticsearch'   then :elasticsearch_loader
        when 'kafka'           then :kafka_loader
        when 'mongo','mongodb' then :mongodb_loader
        when 'sql', 'mysql'    then :sql_loader
        end
      end

    end
  end
end
