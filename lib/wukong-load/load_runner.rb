module Wukong
  module Load

    # Runs the wu-load command.
    class LoadRunner < Wukong::Local::LocalRunner

      usage "DATA_STORE"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-load is a tool for loading data from Wukong into data stores.  It
        supports multiple, pluggable data stores, including:

        Supported data stores:

           elasticsearch
           kafka
           mongodb
           mysql
           zabbix
           hbase (planned)

        Get specific help for a data store with

          $ wu-load store_name --help
      EOF
      
      include Logging

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
      
      # The name of the data store
      #
      # @return [String]
      def data_store_name
        args.first
      end

      # The name of the processor that should handle the data store
      #
      # @return [String]
      def processor
        case data_store_name
        when 'elasticsearch'   then :elasticsearch_loader
        when 'kafka'           then :kafka_loader
        when 'mongo','mongodb' then :mongodb_loader
        when 'sql', 'mysql'    then :sql_loader
        when 'zabbix'          then :zabbix_loader
        end
      end

    end
  end
end
