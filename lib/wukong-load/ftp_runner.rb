require_relative("ftp_runner/ftp_source")

module Wukong
  module Load

    # Runs the wu-ftp command.
    class FTPRunner < Wukong::Runner

      include FTP

      usage ""

      description <<-EOF.gsub(/^ {8}/,'')
        wu-ftp is a tool for transferring data from an FTP/FTPS/SFTP
        server to local disk with a directory structure that is
        compatible for downstream consumption by tools like Storm.

        Here's an example of pulling down some Twitter data from an
        FTP server to the local `data/ftp` directory, under the
        sub-directory `tweets`.

        $ wu-ftp --host=ftp.example.com --path=twitter_data --output=data/ftp --name=tweets
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
          { settings[:name] => FTPSource.new(settings) }
        when settings[:ftp_sources].is_a?(Hash) && args.first
          properties = (settings[:ftp_sources][args.first] || settings[:ftp_sources][args.first.to_sym])
          raise Error.new("Unknown FTP source: <#{args.first}>") unless properties
          { args.first => FTPSource.new(settings.dup.merge({name: args.first}).merge(properties)) }
        when settings[:ftp_sources].is_a?(Hash)
          Hash[settings[:ftp_sources].map { |(name, properties)| [name, FTPSource.new(settings.dup.merge({name: name}).merge(properties))] }]
        else
          raise Error.new("The --ftp_sources settings must be a Hash mapping source names to properties for each source.  Received: #{settings[:ftp_sources].inspect}")
        end
      end

      def run
        sources.each_pair do |name, source|
          paths_processed = source.mirror
          if defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:vayacondios_client) && !paths_processed.empty?
            Wukong::Deploy.vayacondios_client.announce("listeners.ftp_listener-#{name}", paths: paths_processed)
          end
        end
      end

    end
  end
end
