require_relative("sync_runner")
module Wukong
  module Load
    
    # Implements `wu-sync-all`.
    class SyncAllRunner < Wukong::Runner

      include Logging
      include UsesLockfile
      
      usage "DATA_STORE"

      description <<-EOF
wu-sync-all is a tool for running one or many wu-sync commands
simultaneously.  It works with the same data stores as wu-sync:

  ftp
  s3
  archive

It works using a --listeners Hash.  Here's an example of what you
might put in a configuration file:

  ---
  # in config/settings.yml
  listeners:
    nasa:
      ftp:
        host:     ftp.nasa.gov
        username: narmstrong
        password: first!
        path:     /data/latest
      s3:
        bucket: s3://archive.example.com/nasa
    usaf:
      ftp:
        host:     ftp.usaf.gov
        username: bobross
        password: xxx
        path:     /data/latest
      s3:
        bucket: s3://archive.example.com/usaf

This would let you run the following command to sync both `nasa` and
`usaf` listeners from FTP to local disk at the same time:

  $ wu-sync-all ftp --output=/data/ftp

Followed by a command to sync this local disk to S3:

  $ wu-sync-all s3 --input=/data/ftp --bucket=s3://example.com/ftp

For any type of sync, you can control which listeners are synced using
the --only and --except options:

  $ wu-sync-all ftp --output=/data/ftp --only=source_1,source_2
  $ wu-sync-all s3  --output=/data/ftp --except=source_3

The options in the configuration file for each source under each sync
type are identical to the options for wu-sync for that type.  Try

  $ wu-sync SYNC_TYPE --help

for a detailed listing of these options.
EOF

      # Adds options for the syncer class to the settings.
      def configure
        super()
        syncer_klass.configure(settings) if syncer_klass
      end
  
      # Ensure that we were passed a syncer that we know about.
      #
      # @raise [Wukong::Error] if the data store is missing or unknown
      # @return [true]
      def validate
        raise Error.new("Must provide the name of a syncer as the first argument") if syncer_name.nil?
        raise Error.new("No syncer defined for <#{syncer_name}>") if syncer_klass.nil?
        true
      end

      # Run each syncer.
      #
      # Errors from an individual syncer are meant to be handled by
      # the Syncer#run method and re-raised.  This method will ignore
      # those re-raised errors, letting processing continue past a
      # broken syncer.
      def run
        create_lockfile!
        syncers.each do |syncer|
          syncer.run rescue next
        end
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

      # Returns an Array of each syncer used.
      #
      # Respects the `only` and `except`, allowing for more
      # fine-grained control over which listeners are synced.
      #
      # @return [Array<Syncer>]
      def syncers
        @syncers ||= (settings[:listeners] || {}).map do |name, source|
          next if settings[:only]   && !settings[:only].include?(name.to_s)
          next if settings[:except] && settings[:except].include?(name.to_s)
          syncer_klass.from_source(settings, source, name)
        end.compact
      end

      protected

      # We name the lockfile after the type of sync because only one
      # such sync should be running at a given time.
      #
      # @return [String]
      def lockfile_basename
        "sync-all-#{syncer_name}.lock"
      end

    end
  end
end
