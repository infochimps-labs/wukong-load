require_relative("s3_runner/local_source")

module Wukong
  module Load

    # Runs the wu-s3 command.
    #
    # Most of the complexity is contained in the construction of the
    # #sources Hash.  The S3::LocalSource does most of the
    # heavy-lifting.
    class S3Runner < Wukong::Runner

      include Wukong::Load::S3

      LOCKFILE = "./ftp_runner.rb.pid"

      usage "[SOURCE]"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-s3 is a tool for archiving data from local disk to an S3
        bucket.

        Here's an example of archiving a local directory `/data` to
        the S3 bucket 'example.com' under the path '/data':

          $ wu-s3 --input=/data --bucket=s3://example.com/data

        wu-s3 requires the s3cmd program in order to function and the
        above example assumes that the s3cmd tool is already
        configured on the system.

        If s3cmd has not already been configured or if dynamic or
        multiple configurations are required, the `--s3cmd_config`
        file can be used to specify a path to a configuration file for
        s3cmd:

          $ wu-s3 --input=/data --bucket=s3://example.com/data --s3cmd_config=my_aws_account.s3cfg
      EOF
      
      include Logging

      # Delegates to each local source to validate itself.
      #
      # @see S3::LocalSource#validate
      # @return [true]
      # @raise [Wukong::Error] if validation fails
      def validate
        sources.each_value(&:validate)
        true
      end

      # Iterates through and archives each FTp source, handling errors
      # and Vayacondios notification.
      def run
        abort("ERROR: lockfile exists") unless create_lockfile
        begin
          sources.each_pair do |name, source|
            vayacondios_topic = "listeners.ftp_listener-#{name}"
            begin
              source.archive
              if defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:vayacondios_client)
                Wukong::Deploy.vayacondios_client.announce(vayacondios_topic, archived: true, )
              end
            rescue Wukong::Error => e
              log.error(e)
              if defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:vayacondios_client)
                Wukong::Deploy.vayacondios_client.announce(vayacondios_topic, archived: false, error: e.class, message: e.message)
              end
              next
            end
          end
        ensure
          delete_lockfile
        end
      end

      def create_lockfile
        if File.exists?(LOCKFILE)
          false
        else
          File.open(LOCKFILE, "w") do |f|
            pid = Process.pid.to_s
            log.debug "Writing pid #{pid} to lockfile"
            f.write(pid)
          end
        end
      end

      def delete_lockfile
        log.debug "Deleting lockfile"
        File.delete(LOCKFILE)
      end

      # Constructs a Hash of named FTP source credentials using one of three approaches:
      #
      #   1) if a a pre-defined Hash of several named credential sets
      #   is defined (`ftp_sources`), then each of these sources will
      #   be processed
      #
      #   2) if the first command-line argument names one of the
      #   sources defiend in (1) then that source will be run alone
      #
      #   3) if no such sources are defined then rely on the
      #   command-line arguments (`--host`, `--port`, &c.) to define a
      #   source
      #
      # @return [Hash] FTP source names mapped to hashes with credentials for each source
      # @raise [Wukong::Error] in edge cases, e.g. naming a source on the command-line without having defined any prior credentials
      def sources
        case
        when settings[:ftp_sources].nil? && args.first
          raise Error.new("Cannot specify an FTP source by name unless its listed in the `--ftp_sources` setting")
        when settings[:ftp_sources].nil?
          { settings[:name] => LocalSource.new(settings) }
        when settings[:ftp_sources].is_a?(Hash) && args.first
          properties = (settings[:ftp_sources][args.first] || settings[:ftp_sources][args.first.to_sym])
          raise Error.new("Unknown FTP source: <#{args.first}>") unless properties
          { args.first => LocalSource.new(settings.dup.merge({name: args.first}).merge(properties)) }
        when settings[:ftp_sources].is_a?(Hash)
          Hash[settings[:ftp_sources].map { |(name, properties)| [name, LocalSource.new(settings.dup.merge({name: name}).merge(properties))] }]
        else
          raise Error.new("The --ftp_sources settings must be a Hash mapping source names to properties for each source.  Received: #{settings[:ftp_sources].inspect}")
        end
      end
      
    end
  end
end
