require_relative("ftp_runner/ftp_mirror")

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
        mirrors.each_value(&:validate)
        true
      end

      def mirrors
        case
        when settings[:ftp_mirrors].nil? && args.first
          raise Error.new("Cannot specify an FTP source by name unless its listed in the `--ftp_mirrors` setting")
        when settings[:ftp_mirrors].nil?
          { settings[:name] => FTPMirror.new(settings) }
        when settings[:ftp_mirrors].is_a?(Hash) && args.first
          properties = (settings[:ftp_mirrors][args.first] || settings[:ftp_mirrors][args.first.to_sym])
          raise Error.new("Unknown FTP source: <#{args.first}>") unless properties
          { args.first => FTPMirror.new(settings.dup.merge({name: args.first}).merge(properties)) }
        when settings[:ftp_mirrors].is_a?(Hash)
          Hash[settings[:ftp_mirrors].map { |(name, properties)| [name, FTPMirror.new(settings.dup.merge({name: name}).merge(properties))] }]
        else
          raise Error.new("The --ftp_mirrors settings must be a Hash mapping mirror names to properties for each mirror.  Received: #{settings[:ftp_mirrors].inspect}")
        end
      end

      def run
        mirrors.each_pair do |name, mirror|
          paths_processed = mirror.run
          if defined?(Wukong::Deploy) && Wukong::Deploy.respond_to?(:vayacondios_client) && !paths_processed.empty?
            Wukong::Deploy.vayacondios_client.announce("listeners.ftp_listener-#{name}", paths: paths_processed)
          end
        end
      end

    end
  end
end
