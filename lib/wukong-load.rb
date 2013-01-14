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
      return unless program == 'wu-load'
      settings.define :tcp_port, description: "Consume TCP requests on the given port instead of lines over STDIN", type: Integer, flag: 't'
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
require_relative 'wukong-load/runner'

require_relative 'wukong-load/models/http_request'

require_relative 'wukong-load/loaders/elasticsearch'
require_relative 'wukong-load/loaders/kafka'
require_relative 'wukong-load/loaders/mongodb'
