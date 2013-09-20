module Wukong
  module Load
    
    class KafkaDumper < Dumper

      include Logging

      def self.load
        require 'kafka'
      rescue LoadError => e
        raise Error.new("Install the 'kafka-rb' gem to dump data from Kafka")
      end

      def self.validate settings
        raise Error.new("Must provide a Kafka --host to connect to")     if settings[:host].nil? || settings[:host].empty?
        raise Error.new("Must provide a Kafka --port to connect to")     if settings[:port].nil?
        raise Error.new("Must provide a Kafka --topic to read from")     if settings[:topic].nil? || settings[:topic].empty?
        raise Error.new("Must provide a Kafka --partition to read from") if settings[:partition].nil?
      end

      def self.configure settings
        settings.define(:host,            description: "Kafka broker host",       required: true, default: Kafka::IO::HOST)
        settings.define(:port,            description: "Kafka broker port",       required: true, default: Kafka::IO::PORT, type: Integer)
        settings.define(:topic,           description: "Kafka topic to dump",     required: true, default: 'wukong')
        settings.define(:partition,       description: "Kafka partition to dump", required: true, default: 0, type: Integer)
        
        settings.define(:from_beginning,  description: "Read from the beginning of the topic", default: false, type: :boolean)
        settings.define(:offset,          description: "Read from this byte-offset",           type: Integer)
        
        settings.define(:batch_size,      description: "Maximum batch size retrieved (bytes)", type: Integer, default: Kafka::Consumer::MAX_SIZE)
        settings.define(:interval,        description: "Polling interval for new data (seconds)", type: Integer, default: Kafka::Consumer::DEFAULT_POLLING_INTERVAL)

        settings.description = <<-EOF
Start dumping data from the end of a Kafka topic to STDOUT:

  $ wu-dump kafka --topic=foobar
  record 1
  record 2
  ...

Start reading from the beginning of a different partition of the topic
with some more advanced settings:

  $ wu-dump kafka --topic=foobar --partition=2 --from_beginning --batch_size=10485760
  record 1
  record 2
  ...
EOF
      end

      def initialize settings
        super(settings)
        log.debug("Dumping from #{human_offset} of topic <#{settings[:topic]}> on broker <#{settings[:host]}:#{settings[:port]}>")
      end

      def human_offset
        case
        when settings[:offset]         then "offset <#{settings[:offset]}>"
        when settings[:from_beginning] then "the beginning"
        else                                "the end"
        end
      end

      def dump
        # can't use the Kafka::Consumer#loop method here b/c we want
        # to handle errors more cleanly
        messages = []
        while (true) do
          begin
            messages = consumer.consume
            messages.each do |message|
              emit(message.payload)
            end
          rescue => e
            log.error(e)
          end
        end
      end

      def consumer
        @consumer ||= Kafka::Consumer.new(consumer_settings)
      rescue => e
        raise Wukong::Error.new("#{e.class} -- #{e.message}")
      end

      def consumer_settings
        {
          host:      settings[:host],
          port:      settings[:port],
          topic:     settings[:topic],
          partition: settings[:partition],
          max_size:  settings[:batch_size],
          polling:   settings[:interval]
        }.tap do |s|
          s[:offset] = case
          when settings[:offset]         then settings[:offset]
          when settings[:from_beginning] then 0
          end
        end
      end
      
    end
    
  end
end
