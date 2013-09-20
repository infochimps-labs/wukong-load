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
      when 'wu-dump'
      when 'wu-load'
        Wukong::Local.configure(settings, 'wu-local') # configure it just like wu-local
      when 'wu-sync'
        settings.define :dry_run, description: "Don't actually do anything, just print what would happen", type: :boolean, default: false
      when 'wu-sync-all'
        settings.define :sources, description: "Hash of source names to properties used for syncing", type: Hash, default: {}
        settings.define :only,    description: "Comma-separated Array of particular named sources to sync", type: Array
        settings.define :except,  description: "Comma-separated Array of particular named sources to not sync", type: Array
        settings.define :dry_run, description: "Don't actually do anything, just print what would happen", type: :boolean, default: false
      end
    end

    # Boot Wukong-Load from the resolved `settings` in the given
    # `dir`.
    #
    # @param [Configliere::Param] settings the resolved settings
    # @param [String] dir the directory to boot in
    def self.boot settings, dir
    end

    autoload :Loader,       'wukong-load/loaders/loader'
    autoload :Dumper,       'wukong-load/dumpers/dumper'
    autoload :Syncer,       'wukong-load/syncers/syncer'
    
    autoload :UsesLockfile,  'wukong-load/utils/uses_lockfile'
    autoload :UsesFileState, 'wukong-load/utils/uses_file_state'

  end
end
