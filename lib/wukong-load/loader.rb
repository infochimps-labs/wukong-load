module Wukong
  module Load

    # Base class from which to build Loaders.
    class Loader < Wukong::Processor::FromJson

      # Calls super() to leverage its deserialization and then calls
      # #load on the yielded record.
      #
      # @param [String] line JSON to parse.
      def process line
        super(line) { |record| load(record) }
      end

      # Override this method to load a record into the data store.
      #
      # @param [Hash] record
      def load record
      end
      
    end
  end
end
