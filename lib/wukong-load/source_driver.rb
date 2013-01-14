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

      # :nodoc:
      #
      # Not sure why I have to add the call to $stdout.flush at the
      # end of this method.  Supposedly $stdout.sync is called during
      # the #setup method in StdoutProcessor in
      # wukong/widget/processors.  Doesn't that do this?
      def process record
        $stdout.puts record
        $stdout.flush
      end
      
    end
  end
end
