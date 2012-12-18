module Wukong
  module Load
    class Runner

      include Logging

      def self.run settings
        begin
          new(settings).run
        rescue Error => e
          log.error(e.message)
          exit(127)
        end
      end

      attr_accessor :settings
      def initialize settings
        self.settings = settings
      end

      def args
        settings.rest
      end

      def data_store_name
        args.first
      end

      def processor_name
        case data_store_name
        when 'elasticsearch' then :elasticsearch_loader
        when nil
          settings.dump_help
          exit(1)
        else
          raise Error.new("No loader defined for data store: #{data_store_name}")
        end
      end
      
      def run
        EM.run do
          StupidServer.new(processor_name, settings).run!
        end
      end
      
    end
  end
end
