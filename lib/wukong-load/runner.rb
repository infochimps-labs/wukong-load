module Wukong
  module Load

    # Runs the wu-load command.
    class Runner

      include Logging

      # Initialize and begin running a new instance of `wu-load`.
      #
      # @param [COnfigliere:Param] settings
      def self.run settings
        begin
          new(settings).run
        rescue Error => e
          log.error(e.message)
          exit(127)
        end
      end

      # This runner's settings
      attr_accessor :settings

      # Create new instance of `wu-load` with the given settings.
      def initialize settings
        self.settings = settings
      end

      # The command-line args.
      #
      # @return [Array<String>]
      def args
        settings.rest
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

      # Run this loader.
      def run
        EM.run do
          StupidServer.new(processor_name, settings).run!
        end
      end
      
    end
  end
end
