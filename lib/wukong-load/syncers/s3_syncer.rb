module Wukong
  module Load

    # Syncs a local directory to an AWS S3 bucket and path.
    #
    # Uses the [`s3cmd`](http://s3tools.org/s3cmd) tool behind the
    # scenes to do the heavy-lifting.
    class S3Syncer < Syncer

      include Logging

      # List of AWS S3 region names as used by `s3cmd`.
      REGIONS = %w[US EU ap-northeast-1 ap-southeast-1 sa-east-1 us-west-1 us-west-2].freeze

      # Number of bytes synced.
      attr_accessor :bytes
      
      # Number of seconds taken to complete sync.
      attr_accessor :duration

      # Construct a new S3Syncer from the given `settings` for the
      # given `source` with the given `name`.
      #
      # @return [S3Syncer]
      def self.from_source settings, source, name
        raise Error.new("An --input directory is required") unless settings[:input]
        new(settings.dup.merge({
          name:    name.to_s,
          input:   File.join(settings[:input],  name.to_s),
        }.tap { |s| 
          s[:bucket] = File.join(settings[:bucket], name.to_s) if settings[:bucket] 
        }).merge(source[:s3] || {}))
      end
      
      # Configure the `settings` for use with an S3Syncer.
      #
      # @param [Configliere::Param] settings
      def self.configure settings
        settings.define :input,           description: "Local directory to archive to S3"
        settings.define :bucket,          description: "S3 bucket and path to in which to archive data"
        
        settings.define :s3cmd_program,   description: "Path to the `s3cmd` executable", default: 's3cmd'
        settings.define :s3cmd_config,    description: "Path to the `s3cmd` config file"
        settings.define :region,          description: "AWS region to create bucket in, one of: #{REGIONS.join(',')}", default: "US"

        settings.description = <<-EOF
Sync data from a local directory to an S3 bucket and path:

  $ wu-sync s3 --input=/data --bucket=s3://example.com/data

This requires the s3cmd program in order to function and the above
example assumes that the s3cmd tool is already configured correctly
with AWS access key & secret via its standard system configuration
file.

If s3cmd has not already been configured or if dynamic or multiple
configurations are required, the --s3cmd_config option can be used to
specify a path to the right configuration file for s3cmd:

  $ wu-sync s3 --input=/data --bucket=s3://example.com/data --s3cmd_config=my_aws_account.s3cfg
EOF
      end
      
      # Validate the `settings` for this S3 syncer.
      #
      # @raise [Wukong::Error] if the `input` directory is missing or not a directory
      # @raise [Wukong::Error] if the `bucket` is missing
      # @raise [Wukong::Error] if the `s3cmd_config` is missing or not a file
      # @raise [Wukong::Error] if the AWS `region` is invalid
      # @return [true]
      def validate
        raise Error.new("A local --input directory is required") if settings[:input].nil? || settings[:input].empty?
        raise Error.new("Input directory <#{settings[:input]}> does not exist")     unless File.exist?(settings[:input])
        raise Error.new("Input directory <#{settings[:input]}> is not a directory") unless File.directory?(settings[:input])
        
        raise Error.new("An S3 --bucket is required")            if settings[:bucket].nil? || settings[:bucket].empty?

        raise Error.new("s3cmd config file <#{settings[:s3cmd_config]}> does not exist") if settings[:s3cmd_config] && !File.exist?(settings[:s3cmd_config])
        raise Error.new("s3cmd config file <#{settings[:s3cmd_config]}> is not a file")  if settings[:s3cmd_config] && !File.file?(settings[:s3cmd_config])

        raise Error.new("Invalid AWS region <#{settings[:region]}>: must be one of #{REGIONS.join(',')}") unless REGIONS.include?(settings[:region])
        true
      end

      # Setup this S3Syncer.
      def setup
        super()
        require 'shellwords'
      end

      # Log a message.
      def before_sync
        log.info("Syncing from #{local_directory} to #{s3_uri}")
      end

      # Perform the sync.
      #
      # If the `dry_run` setting was given, will just output the
      # `s3cmd` command-line that would have been run.  Otherwise will
      # execute the command-line in a subprocess and log its output at
      # the `DEBUG` level.
      def sync
        if settings[:dry_run]
          log.info(sync_command)
        else
          IO.popen(sync_command).each { |line| handle_output(line) }
        end
      end

      # The local filesystem directory to use as the source for the
      # sync.
      #
      # @return [String]
      def local_directory
        File.join(File.expand_path(settings[:input].to_s), "")  # always use a trailing '/' with s3cmd
      end

      # The S3 bucket and path to use as the destination for the sync.
      #
      # The `s3://` scheme will be added if not already present.
      #
      # @return [String]
      def s3_uri
        File.join(settings[:bucket] =~ %r{^s3://}i ? settings[:bucket].to_s : "s3://#{settings[:bucket]}", "") # always use a traling '/' with s3cmd
      end

      # The command that will be run.
      def sync_command
        config_file = settings[:s3cmd_config] ? "--config=#{Shellwords.escape(settings[:s3cmd_config])}" : ""
        "#{s3cmd_program} sync #{Shellwords.escape(local_directory)} #{Shellwords.escape(s3_uri)} --no-delete-removed --bucket-location=#{settings[:region]} #{config_file} 2>&1"
      end

      # The path to the `s3cmd` program.
      #
      # Will use the `s3cmd_program` setting if given, otherwise
      # defaults to `s3cmd`.
      #
      # @return [String]
      def s3cmd_program
        settings[:s3cmd_program] || 's3cmd'
      end

      private

      # :nodoc:
      def handle_output line
        case
        when line =~ /^Done. Uploaded (\d+) bytes in ([\d\.]+) seconds/
          self.bytes     = $1.to_i
          self.duration  = $2.to_f
          log.debug(line.chomp)
        when line =~ /^ERROR:\s+(.*)$/
          raise Error.new($1.chomp)
        else
          log.debug(line.chomp)
        end
      end
      
    end
  end
end
