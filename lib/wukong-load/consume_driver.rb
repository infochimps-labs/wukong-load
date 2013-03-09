module Wukong
  module Load

    # FIXME -- replace this class with one that uses EM::Kafka.
    class ConsumeDriver
      
      include Wukong::DriverMethods
      include Wukong::Processor::StdoutProcessor
      def setup() ; end

      include Wukong::Logging

      def initialize label, settings
        super
        @settings = settings      
        @dataflow = construct_dataflow(label, settings)
      end
      
      def self.start(label, settings = {})
        new(label, settings).start
      end

      def consumer_options
        {host: settings[:host], port: settings[:port], topic: settings[:topic], partition: settings[:partition]}.tap do |o|
          o[:offset] = settings[:offset] if settings[:offset]
        end
      end

      def process message
        if message.is_a?(String)
          puts message
        else
          yield message.payload.to_s
        end
      end

      def start
        Signal.trap('INT')  { log.info 'Received SIGINT. Stopping.'  ; finalize_and_stop_dataflow ; exit(1) }
        Signal.trap('TERM') { log.info 'Received SIGTERM. Stopping.' ; finalize_and_stop_dataflow ; exit(1) }
        setup_dataflow
        Kafka::Consumer.new(consumer_options).loop do |messages|
          messages.each do |message|
            begin
              driver.send_through_dataflow(message.payload.to_s)
            rescue => e
              raise Wukong::Error.new(e)
            end
          end
        end
      end
      
    end
  end
end
