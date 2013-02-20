require_relative('../loader')

module Wukong
  module Load

    # Loads data into Kafka.
    #
    # Uses the `kafka-rb` gem to create a Kafka::Producer to write to
    # Kafka.
    #
    # Allows loading records into a given topic on a given partition.
    # Records can have fields `_topic` and `_partition` which override
    # the given topic and partition on a per-record basis.
    #
    # The names of these fields within each record (`_topic` and
    # `_partition`) can be customized.
    class KafkaLoader < Loader

      field :host,            String,  :default => 'localhost',  :doc => "Kafka broker host"
      field :port,            Integer, :default => 9092,         :doc => "Kafka broker port"
      field :topic,           String,  :default => 'test',       :doc => "Kafka topic"
      field :topic_field,     String,  :default => '_topic',     :doc => "Field within records which names the Kafka topic"
      field :partition,       Integer, :default => 0,            :doc => "Kafka partition"
      field :partition_field, String,  :default => '_partition', :doc => "Field within records which names the Kafka partition"

      description <<-EOF.gsub(/^ {8}/,'')
        Loads newline-separated, JSON-formatted records over STDIN
        into a Kafka queue.

          $ cat data.json | wu-load kafka

        By default, wu-load attempts to write each input record to a
        local Kafka broker.

        Input records will be written to a default Kafka topic on a
        default partition.  Each record can have _topic and _partition
        fields to override this on a per-record basis.

        The fields used (_topic and _partition) can be changed:

          $ cat data.json | wu-load kafka --host=10.123.123.123 --topic=hits --partition_field=segment_id
      EOF

      # The Kafka producer used to send messages to Kafka.
      attr_accessor :producer

      # Creates the producer.
      def setup
        begin
          require 'kafka'
        rescue => e
          raise Error.new("Please ensure that the 'kafka-rb' gem is installed and available (in your Gemfile)")
        end
        log.debug("Connecting to Kafka broker at #{host}:#{port}...")
        begin
          self.producer = Kafka::MultiProducer.new(:host => host, :port => port)
        rescue => e
          raise Error.new(e.message)
        end
      end

      # Load a single record into Kafka.
      #
      # @param [Hash] record
      def load record
        begin
          topic     = topic_for(record)
          partition = partition_for(record)
          bytes     = producer.send(topic, messages_for(record), :partition => partition)
          log.info("Wrote #{bytes} bytes to #{topic}/#{partition}")
        rescue => e
          handle_error(record, e)
        end
      end

      # :nodoc:
      def topic_for record
        record[topic_field] || self.topic
      end

      # :nodoc:
      def messages_for record
        [Kafka::Message.new(MultiJson.dump(record))]
      end

      # :nodoc:
      def partition_for record
        record[partition_field] ? record[partition_field].to_i : partition
      end
      
      register :kafka_loader
      
    end
  end
end

    
    
