module Wukong
  module Load

    # Loads data into Kafka.
    #
    # Uses the `kafka-rb` gem to create a Kafka::Producer to write to
    # Kafka.
    #
    # Allows loading records into a given topic on a given partition.
    class KafkaLoader < Wukong::Processor

      field :host,            String,  :default => 'localhost',  :doc => "Kafka broker host"
      field :port,            Integer, :default => 9092,         :doc => "Kafka broker port"
      field :topic,           String,  :default => 'wukong',     :doc => "Kafka topic"
      field :partition,       Integer, :default => 0,            :doc => "Kafka partition"
      field :batch_size,      Integer, :default => 200,          :doc => "Number of messages to send at once"

      description <<-EOF
Loads newline-separated records over STDIN into a Kafka topic:

  $ cat data.json | wu-load kafka

By default, wu-load attempts to write each input record to a local
Kafka broker on the 0th partition of the `test` topic, but these
options can all be changed:

  $ cat data.json | wu-load kafka --host=10.123.123.123 --topic=hits --partition=3
EOF

      # The Kafka producer used to send messages to Kafka.
      attr_accessor :producer

      # The batch of messages we're building.
      attr_accessor :messages

      # Creates the producer.
      def setup
        begin
          require 'kafka'
        rescue LoadError => e
          raise Error.new("Please ensure that the 'kafka-rb' gem is installed and available (in your Gemfile)")
        end
        log.debug("Connecting to Kafka broker at #{host}:#{port}...")
        begin
          self.producer = Kafka::Producer.new(:host => host, :port => port, :topic => topic, :partition => partition)
        rescue => e
          raise Error.new(e.message)
        end
        self.messages = []
      end

      # Load a single line into Kafka.
      #
      # @param [String] line the line that will be loaded
      def process line
        self.messages << line
        load if messages.size >= batch_size
      end
      
      # Load the batch.
      def load
        begin
          producer.batch do |batch|
            messages.each do |message|
              batch << Kafka::Message.new(message)
            end
          end
          log.debug("Wrote #{messages.size} messages to #{topic}/#{partition}")
          messages.clear
        rescue => e
          handle_error(e)
        end
      end

      def finalize
        load
      end

      # :nodoc:
      def handle_error(err)
        return if err.class == Errno::EPIPE
        log.error "#{err.class}: #{err.message}"
        err.backtrace.each { |line| log.debug(line) }
      end
      
      register :kafka_loader
      
    end
  end
end

    
    
