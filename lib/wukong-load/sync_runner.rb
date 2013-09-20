module Wukong
  module Load
    
    autoload :FTPSyncer,     'wukong-load/syncers/ftp_syncer'
    autoload :S3Syncer,      'wukong-load/syncers/s3_syncer'
    autoload :ArchiveSyncer, 'wukong-load/syncers/archive_syncer'
    
    # Implements `wu-sync`.
    class SyncRunner < Wukong::Runner

      include Logging
      include UsesLockfile
      
      usage "DATA_STORE"

      description <<-EOF
wu-sync is a tool for syncing data between a local filesystem and
other pluggable filesystem-like data stores including:

  ftp -- syncs to a remote FTP/FTPS/SFTP directory
  s3 -- syncs to AWS S3 bucket and path
  archive -- syncs non-growing files locally with hardlinks

For more help on a specific sycner, run:

  $ wu-sync SYNC_TYPE --help
EOF

      # Ensure that we were passed a data store name that we know
      # about.
      #
      # @raise [Wukong::Error] if the data store is missing or unknown
      # @return [true]
      def validate
        case
        when syncer_name.nil?
          raise Error.new("Must provide the name of a syncer as the first argument")
        when syncer_klass.nil?
          raise Error.new("No syncer defined for <#{syncer_name}>")
        end
        true
      end

      # Adds syncer specific options to the settings.
      def configure
        super()
        syncer_klass.configure(settings) if syncer_klass
      end

      # Start up and run the syncer.
      def run
        create_lockfile!
        syncer.run
      ensure
        delete_lockfile!
      end

      # The name of the data store
      #
      # @return [String]
      def syncer_name
        ARGV.detect { |arg| arg !~ /^--/ }
      end
      
      # The name of the syncer.
      #
      # @return [Class, nil] the syncer name or `nil` if no such syncer exists
      def syncer_klass
        case syncer_name.to_s.downcase
        when 's3'         then S3Syncer
        when 'ftp'        then FTPSyncer
        when 'archive'    then ArchiveSyncer
        end
      end

      # The syncer instance.
      #
      # @return [Syncer]
      def syncer
        @syncer ||= syncer_klass.new(settings)
      end

      protected

      # The name of the lockfile to use.  Scoped by the syncer type so
      # that different types of syncers can run at the same time.
      #
      # @return [String]
      def lockfile_basename
        "sync-#{syncer_name}.lock"
      end

    end
  end
end
