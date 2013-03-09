require_relative("consume_driver")
module Wukong
  module Load
    class ConsumeRunner < Wukong::Local::LocalRunner

      include Logging
      
      usage "PROCESSOR|DATAFLOW"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-consume is a tool for running Wukong dataflows as consumers
        of data on a Kafka queue:

          $ wu-consume --topic=tweets my_twitter_parser

        It's especially useful in combination with loaders defined by
        wu-load:

          $ wu-consume --topic=tweets mongodb_loader
    EOF

      def driver
        ConsumeDriver
      end
    end
  end
  
end
