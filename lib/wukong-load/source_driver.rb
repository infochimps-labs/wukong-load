module Wukong
  module Load
    class SourceDriver < Wukong::Local::StdioDriver
      include Logging

      attr_accessor :index

      def post_init
        super()
        self.index = 1
      end

      def self.start(label, settings={})
        driver = new(:foobar, label, settings)
        driver.post_init
        period = (1.0 / (settings[:per_sec] || 1.0))
        EventMachine::PeriodicTimer.new(period) { driver.create_event }
      end

      def create_event
        receive_line(index.to_s)
        self.index += 1
      end
      
    end
  end
end
