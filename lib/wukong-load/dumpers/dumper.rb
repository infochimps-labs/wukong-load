module Wukong
  module Load

    # A base class for all dumpers to subclass.
    class Dumper

      include Logging

      attr_accessor :settings

      # Create a new dumper.
      # 
      # @param [Configliere::Param] settings
      def initialize settings
        self.settings   = settings
      end

      def self.load
      end

      def self.validate settings
        true
      end

      def self.configure settings
      end
      
      def setup
      end

      # Dump the data.
      def dump
        raise NotImplementedError.new("Override the #{self.class}#dump method")
      end

      # Write `record` to STDOUT.
      #
      # @param [#to_s] record
      def emit record
        $stdout.puts(record.to_s)
      end
      
    end
  end
end
