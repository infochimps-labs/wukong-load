require 'wukong'

module Wukong
  # Loads data from the command-line into data stores.
  module Load
    include Plugin

    # Configure `settings` for Wukong-Load.
    #
    # @param [Configliere::Param] settings the settings to configure
    # @param [String] program the currently executing program name
    def self.configure settings, program
      case program
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
      when 'wu-ftp'
        settings.define :protocol, description: "Transfer protocol to use: one of ftp, ftps, sftp", default: "ftp"
        settings.define :host,     description: "Host to login to", default: "localhost"
        settings.define :port,     description: "Port to login to.  Default depends on the protocol", type: Integer
        settings.define :username, description: "Username to login as"
        settings.define :password, description: "Password to use when logging in", env_var: "FTP_PASSWORD"
        settings.define :path,     description: "Path on server to download", default: "/"

        settings.define :output,   description: "Local root directory for downloaded data"
        settings.define :name,     description: "Name of download subdirectory for data"
        settings.define :links,    description: "Local directory for lexicographically ordered links to data"
        settings.define :dry_run,  description: "Don't actually download anything from the server", type: :boolean, default: false

        settings.define :ftp_mirrors, type: Hash, description: "Hash mapping names to settings for mirroring each server"

        settings.define :lftp_program,      description: "Path to the `lftp` executable", default: 'lftp'
        settings.define :ignore_unverified, description: "Ignore errors due to an unverifiable (self-signed) SSL certificate", type: :boolean, default: false
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
