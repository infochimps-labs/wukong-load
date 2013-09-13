require_relative("default_file_handler")

module Wukong
  module Load
    module FTP

      # Describes a source of FTP data.
      #
      # Design goals include
      #
      # * provide an abstraction between the various and confusing types of FTP service (SFTP vs FTPS)
      # * support "once and exactly once" processing for **whole files**
      # * be part of a pluggable toolchain
      # * integrate well with logging and notification systems (e.g. Vayacondios)
      #
      # @example Mirror a local FTP server using an anonymous account
      #
      #   ftp_source = FTPSource.new(name: 'local-root', output: '/tmp/ftp/raw', links: '/tmp/ftp/clean')
      #   newly_downloaded_paths = ftp_source.mirror # downloads files and updates hardlinks (if necessary)
      #
      # @example Mirror a remote SFTP server using the account 'bob'
      #
      #   ftp_source = FTPSource.new(name: 'west-coast', protocol: 'sftp', host: 'ftp.example.com', username: 'bob', password: 'ross', path: '/systemX/2017/03', output: '/tmp/ftp/raw', links: '/tmp/ftp/clean')
      #   newly_downloaded_paths = ftp_source.mirror # downloads files and updates hardlinks (if necessary)
      #
      # Depends upon the [`lftp`](http://lftp.yar.ru/) tool being
      # available on the system.
      class FTPSource

        include Logging

        # A mapping between protocal names and the standard ports
        # those services run on.
        PROTOCOLS = {
          'ftp'  => 21,
          'ftps' => 443,
          'sftp' => 22
        }.freeze

        # The verbosity level passed to the `lftp` program
        VERBOSITY = 3

        attr_accessor :settings

        # Create a new FTP source.
        #
        # @example Create a source for a local FTP server using an anonymous account
        #
        #   Wukong::Load::FTP::FTPSource.new(name: 'local-root', output: '/tmp/ftp/raw', links: '/tmp/ftp/clean')
        #
        # @example Create a source for a remote SFTP server using the account 'bob'
        #
        #   Wukong::Load::FTP::FTPSource.new(name: 'west-coast', protocol: 'sftp', host: 'ftp.example.com', username: 'bob', password: 'ross', path: '/systemX/2017/03', output: '/tmp/ftp/raw', links: '/tmp/ftp/clean')
        #
        def initialize settings
          self.settings = settings
        end

        # Validates this FTP source.  Checks
        #
        # * the protocol is valid (one of `ftp`, `ftps`, or `sftp`)
        # * a host is given
        # * a path is given
        # * a local output directory for downlaoded data is given
        # * a local links directory for lexicographically ordered hardlinks to data is given
        # * a name is given
        # 
        # @return [true] if the source if valid
        # @raise [Wukong::Error] if the source is not valid
        def validate
          raise Error.new("Unsupported --protocol: <#{settings[:protocol]}>") unless PROTOCOLS.include?(settings[:protocol])
          raise Error.new("A --host is required") if settings[:host].nil? || settings[:host].empty?
          raise Error.new("A --path is required") if settings[:path].nil? || settings[:path].empty?
          raise Error.new("A local --output directory is required") if settings[:output].nil? || settings[:output].empty?
          raise Error.new("A local --links directory is required")  if settings[:links].nil? || settings[:links].empty?
          raise Error.new("The --name of a directory within the output directory is required") if settings[:name].nil? || settings[:name].empty?
          true
        end

        # The port to use for this FTP source.
        #
        # If we were given an explicit port, then use that, otherwise
        # use the standard port given the protocol.
        #
        # @return [Integer] the port
        def port
          settings[:port] || PROTOCOLS[settings[:protocol]]
        end

        # Mirror the content at the remote FTP server to the local
        # output directory and create a lexicographically ordered
        # representation of this data in the links directory.
        #
        # @see #file_handler for the class which constructs local hardlinks based on remote FTP paths
        # @return [Array<String>] the newly mirrored remote FTP paths
        def mirror
          user_msg = settings[:username] ? "#{settings[:username]}@" : ''
          log.info("Mirroring #{settings[:protocol]} #{user_msg}#{settings[:host]}:#{port}#{settings[:path]}")
          command = send("#{settings[:protocol]}_command")
          if settings[:dry_run]
            log.info(command)
            []
          else
            subprocess      = IO.popen(command)
            paths_processed = []
            path_index      = 0
            subprocess.each do |line|
              handle_output(path_index, paths_processed, line)
            end
          end
          file_handler.close
          paths_processed
        end

        # Handle a line of input given the context of the otuput.
        #
        # Will increment `path_index` and add to `paths_processed` if
        # `line` indicates a new path was transferred.
        #
        # Will also log the line.
        #
        # @param [Integer] path_index the current index of the last newly mirrored path
        # @param [Array<String>] the paths which have already been mirrored
        # @param [String] a new line of output from `lftp`
        def handle_output path_index, paths_processed, line
          log.debug(line.chomp)
          if path = newly_downloaded_path?(line)
            file_handler.process(path, path_index)
            paths_processed << path
            path_index      += 1
          end
        end


        # Handle a file that has been deemed finished and ready for final processing
        #
        # Delegates to the `file_handler` for final processing
        #
        # @param [String] filename
        def handle_finished_file filename
          file_handler.process_finished filename
        end

        # Is the file corresponding to `filename` completely transferred to the
        # remote FTP server, and do we believe that it is faithfully mirrored
        # locally?
        #
        # Since we are using lftp mirror mode, the best we can do is check
        # whether a file we have processed during the previous run has been
        # processed again, and if so, do not consider it finished
        #
        # @param [String] filename
        # @param [Array] paths_processed
        # @return [Boolean]
        def finished? filename, paths_processed
          !paths_processed.index(filename)
        end

        # Does the `line` indicate a newly downloaded path from the
        # remote FTP server?
        #
        # @param [String] line
        # @return [String, nil] the path that was downloaded or `nil` if none was
        def newly_downloaded_path? line
          return unless line.include?("Transferring file")
          return unless line =~ /`(.*)'/
          $1
        end

        # The file handler that will process each newly downloaded
        # path and create appropriate hardlinks on disk.
        #
        # The FTPFileHandler is the default.
        #
        # @return [FTPFileHandler]
        # @see FTPFileHandler
        def file_handler
          @file_handler ||= FTPFileHandler.new(settings)
        end

        # The command to use when using the FTP protocol.
        #
        # @return [String]
        def ftp_command
          lftp_command
        end

        # The command to use when using the FTPS protocol.
        #
        # @return [String]
        def ftps_command
          lftp_command('set ftps:initial-prot "";', 'set ftp:ssl-force true;', 'set ftp:ssl-protect-data true;')
        end

        # The command to use when using the SFTP protocol.
        #
        # @return [String]
        def sftp_command
          lftp_command
        end

        # Construct an `lftp` command-line from the settings for this
        # source as well as the given `subcommands`.
        #
        # @param [Array<String>] subcommands each terminated with a semi-colon (`;')
        def lftp_command *subcommands
          command = ["#{lftp_program} -c 'open -e \""]
          command += subcommands
          command << "set ssl:verify-certificate no;" if settings[:ignore_unverified]
          command << "mirror --verbose=#{VERBOSITY} #{settings[:path]} #{settings[:output]}/#{settings[:name]};"
          command << "exit"
          
          auth = ""
          if settings[:username] || settings[:password]
            auth += "-u "
            if settings[:username]
              auth += settings[:username]
              if settings[:password]
                auth += ",#{settings[:password]}"
              end
              auth += " "
            end
          end
          command << "\" -p #{port} #{auth} #{settings[:protocol]}://#{settings[:host]}'"
          command.flatten.compact.join(" \t\\\n  ")
        end

        # The path on disk for the `lftp` program.
        #
        # @return [String]
        def lftp_program
          settings[:lftp_program] || 'lftp'
        end
        
      end
    end
  end
end

