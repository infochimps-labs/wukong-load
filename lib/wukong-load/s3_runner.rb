require_relative("s3_runner/local_source")

module Wukong
  module Load

    # Runs the wu-s3 command.
    class S3Runner < Wukong::Runner

      include Wukong::Load::S3

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

      def validate
        sources.each_value(&:validate)
        true
      end

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

      def run
        sources.each_pair do |name, source|
          begin
            source.archive
            if defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:vayacondios_client)
              Wukong::Deploy.vayacondios_client.announce("archivers.ftp_archiver-#{name}", success: true)
            end
          rescue Wukong::Error => e
            log.error(e)
            next
          end
        end
      end

    end
  end
end
