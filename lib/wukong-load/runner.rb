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
           hbase (planned)
           mongob (planned)
           mysql (planned)

        Get specific help for a data store with

          $ wu-load store_name --help

        Elasticsearch Usage:

        Pass newline-separated, JSON-formatted records over STDIN:

        $ cat data.json | wu-load elasticsearch

        By default, wu-load attempts to write each input record to a local
        Elasticsearch database.  Records will be routed to a default
        Elasticsearch index and type.  Records with an '_id' field will be
        considered updates.  The rest will be creates.  You can override these
        options:

        $ cat data.json | wu-load elasticsearch --host=10.123.123.123 --index=my_app --es_type=my_obj --id_field="doc_id"

        Params:
           --host=String            Elasticsearch host, without HTTP prefix [Default: localhost]
           --port=Integer           Port on Elasticsearch host [Default: 9200]
           --index=String           Default Elasticsearch index for records [Default: wukong]
           --es_type=String         Default Elasticsearch type  for records [Default: streaming_record]
           --index_field=String     Field in each record naming desired Elasticsearch index
           --es_type_field=String   Field in each record naming desired Elasticsearch type
           --id_field=String        Field in each record naming providing ID of existing Elasticsearch record to update
      EOF
      
      include Logging
      
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
        when 'elasticsearch' then :elasticsearch_loader
        end
      end

    end
  end
end
