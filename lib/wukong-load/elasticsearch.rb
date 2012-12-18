# This should be extracted into Wonderdog and inserted via the Wukong
# plugin mechanism.

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

      attr_accessor :connection

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

      def load record
        id_for(record) ? request(Net::HTTP::Put, update_path(record), record) : request(Net::HTTP::Post, create_path(record), record)
      end

      def create_path record
        File.join('/', index_for(record).to_s, es_type_for(record).to_s)
      end

      def update_path record
        File.join('/', index_for(record).to_s, es_type_for(record).to_s, id_for(record).to_s)
      end
      
      def index_for record
        record[index_field] || self.index
      end

      def es_type_for record
        record[es_type_field] || self.es_type
      end

      def id_for record
        record[id_field]
      end

      def request request_type, path, record
        perform_request(create_request(request_type, path, record))
      end

      private
      
      def create_request request_type, path, record
        request_type.new(path).tap do |req|
          req.body = MultiJson.dump(record)
        end
      end

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

      def handle_elasticsearch_error response
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

    
    
