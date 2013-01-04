require_relative('loader')

module Wukong
  module Load

    # Loads data into Elasticsearch
    class ElasticsearchLoader < Loader

      field :host,          String, :default => 'localhost'
      field :port,          Integer,:default => 9200
      field :index,         String, :default => 'wukong'
      field :es_type,       String, :default => 'streaming_record'
      field :index_field,   String, :default => '_index'
      field :es_type_field, String, :default => '_es_type'
      field :id_field,      String, :default => '_id'

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
      # @param [record] Hash
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

    
    
