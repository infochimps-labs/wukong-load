module Wukong
  module Load
    class SourceDriver < Wukong::Local::StdioDriver
      include Logging

      attr_accessor :index, :batch_size

      def post_init
        super()
        self.index = 1
        self.batch_size = settings[:batch_size].to_i if settings[:batch_size] && settings[:batch_size].to_i > 0
      end

      def self.start(label, settings={})
        driver = new(:foobar, label, settings)
        driver.post_init

        period = case
        when settings[:period]  then settings[:period]
        when settings[:per_sec] then (1.0 / settings[:per_sec]) rescue 1.0
        else 1.0
        end
        driver.create_event
        EventMachine::PeriodicTimer.new(period) { driver.create_event }
      end

      def create_event
        receive_line(index.to_s)
        self.index += 1
        finalize_dataflow if self.batch_size && (self.index % self.batch_size) == 0
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
