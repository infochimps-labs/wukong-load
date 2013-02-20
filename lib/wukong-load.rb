require 'wukong'

module Wukong
  # Loads data from the command-line into data stores.
  module Load
    include Plugin

    # Configure `settings` for Wukong-Load.
    #
    # Will ensure that `wu-load` has the same settings as `wu-local`.
    #
    # @param [Configliere::Param] settings the settings to configure
    # @param [String] program the currently executing program name
    def self.configure settings, program
      case program
      when 'wu-load'
        settings.define :tcp_port, description: "Consume TCP requests on the given port instead of lines over STDIN", type: Integer, flag: 't'
      when 'wu-source'
        settings.define :per_sec,    description: "Number of events produced per second", type: Float
        settings.define :period,     description: "Number of seconds between events (overrides --per_sec)", type: Float
        settings.define :batch_size, description: "Trigger a finalize across the dataflow each time this many records are processed", type: Integer
      when 'wu-consume'
        settings.define :topic,          description: "Kafka topic to consume data from", default: 'wukong'
        settings.define :partition,      description: "Kafka partition to consume data from", default: 0, type: Integer
        settings.define :host,           description: "Host for Kafka server", default: 'localhost'
        settings.define :port,           description: "Port for Kafka server", default: 9092
        settings.define :period,         description: "Number of seconds between requests for data from Kafka", default: 10
        settings.define :offset,         description: "Specify the offset to start from, set 0 to read from beginning"
      end
    end

    # Boot Wukong-Load from the resolved `settings` in the given
    # `dir`.
    #
    # @param [Configliere::Param] settings the resolved settings
    # @param [String] dir the directory to boot in
    def self.boot settings, dir
    end
    
  end
end
require_relative 'wukong-load/load_runner'
require_relative 'wukong-load/source_runner'

require_relative 'wukong-load/models/http_request'

require_relative 'wukong-load/loaders/elasticsearch'
require_relative 'wukong-load/loaders/kafka'
require_relative 'wukong-load/loaders/mongodb'
require_relative 'wukong-load/loaders/sql'
