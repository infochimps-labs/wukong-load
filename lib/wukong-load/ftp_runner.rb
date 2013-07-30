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
        mirrors.each(&:validate)
        true
      end

      def mirrors
        case
        when settings[:ftp_mirrors].nil?
          [FTPMirror.new(settings)]
        when settings[:ftp_mirrors].is_a?(Hash)
          settings[:ftp_mirrors].map do |(name, properties)|
            FTPMirror.new(settings.dup.merge({name: name}).merge(properties))
          end
        else
          raise Error.new("The --ftp_mirrors settings must be a Hash mapping mirror names to properties for each mirror.  Received: #{settings[:ftp_mirrors].inspect}")
        end
      end

      def run
        mirrors.each(&:mirror)
      end

    end
  end
end
