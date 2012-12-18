module Wukong
  module Load

    # Base class from which to build Loaders.
    class Loader < Wukong::Processor::FromJson

      def process line
        super(line) { |record| load(record) }
      end

      def load record
      end
      
    end
  end
end
