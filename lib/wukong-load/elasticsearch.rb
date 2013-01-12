require_relative('loader')

module Wukong
  module Load

    # Loads data into Elasticsearch.
    #
    # Uses Elasticsearch's HTTP API to communicate.
    #
    # Allows loading records into a given index and type.  Records can
    # have fields `_index` and `_es_type` which override the given
    # index and type on a per-record basis.
    #
    # Records can have an `_id` field which indicates an update, not a
    # create.
    #
    # The names of these fields within each record (`_index`,
    # `_es_type`, and `_id`) can be customized.
    class ElasticsearchLoader < Loader

      field :host,          String, :default => 'localhost', :doc => "Elasticsearch host"
      field :port,          Integer,:default => 9200, :doc => "Port on Elasticsearch host"
      field :index,         String, :default => 'wukong', :doc => "Default Elasticsearch index for records"
      field :es_type,       String, :default => 'streaming_record', :doc => "Default Elasticsearch type for records"
      field :index_field,   String, :default => '_index', :doc => "Name of field in each record overriding default Elasticsearch index"
      field :es_type_field, String, :default => '_es_type', :doc => "Name of field in each record overriding default Elasticsearch type"
      field :id_field,      String, :default => '_id', :doc => "Name of field in each record providing ID of existing Elasticsearch record to update"

      description <<-EOF.gsub(/^ {8}/,'')
        Loads newline-separated, JSON-formatted records over STDIN
        into Elasticsearch using its HTTP API.

          $ cat data.json | wu-load elasticsearch

        By default, wu-load attempts to write each input record to a
        local Elasticsearch database.

        Input records will be written to a default Elasticsearch index
        and type.  Each record can have _index and _es_type fields to
        override this on a per-record basis.

        Records with an _id field will be trigger updates, the rest
        creates.

        The fields used (_index, _es_type, and _id) can be changed:

          $ cat data.json | wu-load elasticsearch --host=10.123.123.123 --index=web_events --es_type=impressions --id_field="impression_id"
      EOF

      # The Net::HTTP connection we'll use for talking to
      # Elasticsearch.
      attr_accessor :connection

      # Creates a connection
      def setup
        h = host.gsub(%r{^http://},'')
        log.debug("Connecting to Elasticsearch cluster at #{h}:#{port}...")
        begin
          self.connection = Net::HTTP.new(h, port)
          self.connection.use_ssl = true if host =~ /^https/
        rescue => e
          raise Error.new(e.message)
        end
      end

      # Load a single record into Elasticsearch.
      #
      # If the record has an ID, we'll issue an update, otherwise a create
      #
      # @param [Hash] record
      def load record
        id_for(record) ? request(Net::HTTP::Put, update_path(record), record) : request(Net::HTTP::Post, create_path(record), record)
      end

      # :nodoc:
      def create_path record
        File.join('/', index_for(record).to_s, es_type_for(record).to_s)
      end

      # :nodoc:
      def update_path record
        File.join('/', index_for(record).to_s, es_type_for(record).to_s, id_for(record).to_s)
      end

      # :nodoc:
      def index_for record
        record[index_field] || self.index
      end

      # :nodoc:
      def es_type_for record
        record[es_type_field] || self.es_type
      end

      # :nodoc:
      def id_for record
        record[id_field]
      end

      # Make a request via the existing #connection.  Record will be
      # turned to JSON automatically.
      #
      # @param [Net::HTTPRequest] request_type
      # @param [String] path
      # @param [Hash] record
      def request request_type, path, record
        perform_request(create_request(request_type, path, record))
      end

      private

      # :nodoc:
      def create_request request_type, path, record
        request_type.new(path).tap do |req|
          req.body = MultiJson.dump(record)
        end
      end
      
      # :nodoc:
      def perform_request req
        begin
          response = connection.request(req)
          status   = response.code.to_i
          if (200..201).include?(status)
            log.info("#{req.class} #{req.path} #{status}")
          else
            handle_elasticsearch_error(status, response)
          end
        rescue => e
          log.error("#{e.class} - #{e.message}")
        end
      end

      # :nodoc:      
      def handle_elasticsearch_error status, response
        begin
          error = MultiJson.load(response.body)
          log.error("#{response.code}: #{error['error']}")
        rescue => e
          log.error("Received a response code of #{status}: #{response.body}")
        end
      end
        
      register :elasticsearch_loader
      
    end
  end
end

    
    
