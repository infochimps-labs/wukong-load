module Wukong
  module Load

    # Syncs a directory on an FTP/FTPS/SFTP server to a local
    # directory.
    #
    # Uses the [`lftp`](http://lftp.yar.ru/) tool behind the scenes to
    # do the heavy-lifting.
    class FTPSyncer < Syncer

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

      # Construct a new FTPSyncer from the given `settings` for the given
      # `source` with the given `name`.
      #
      # @return [FTPSyncer]
      def self.from_source settings, source, name
        raise Error.new("An --output directory is required") unless settings[:output]
        new(settings.dup.merge({
          name:         name.to_s,
          output:       File.join(settings[:output], name.to_s),
        }).merge(source[:ftp] || {}))
      end
      
      # Configure the `settings` for use with an FTPSyncer.
      #
      # @param [Configliere::Param] settings
      def self.configure settings
        settings.define :protocol, description: "Transfer protocol to use: one of ftp, ftps, sftp", default: "ftp"
        settings.define :host,     description: "Host to login to", default: "localhost"
        settings.define :port,     description: "Port to login to.  Default depends on the protocol", type: Integer
        settings.define :username, description: "Username to login as"
        settings.define :password, description: "Password to use when logging in", env_var: "FTP_PASSWORD"
        settings.define :path,     description: "Path on server to download", default: "/"

        settings.define :output, description: "Local root directory for downloaded data"
        
        settings.define :lftp_program, description: "Path to the `lftp` executable", default: 'lftp'
        
        settings.define :ignore_unverified, description: "Ignore errors due to an unverifiable (self-signed) SSL certificate", type: :boolean, default: false
        
        settings.description = <<-EOF
Syncs an FTP/FTPS/SFTP server to a local directory:

  $ wu-sync ftp --host=ftp.example.com --output=/data/ftp

Files in the remote directory will be mirrored based on their current
state, including files currently being uploaded.

The protocol, host, and credentials can all be changed with flags:

  $ wu-sync ftp --host=ftp.example.com --port=1234 --protocol=ftps --username=john --password=xxx --output=/data/ftp

`wu-ftp` requires the `lftp` program in order to function.
EOF
      end

      # Validate the `settings` for this S3 syncer.
      #
      # @raise [Wukong::Error] if the `protocol` is invalid
      # @raise [Wukong::Error] if the `host` is missing or empty
      # @raise [Wukong::Error] if the `path` is missing or empty
      # @raise [Wukong::Error] if the local `output` directory exists but is not a directory
      # @return [true]
      def validate
        raise Error.new("Unsupported --protocol: <#{settings[:protocol]}>") unless PROTOCOLS.include?(settings[:protocol])
        raise Error.new("A --host is required") if settings[:host].nil? || settings[:host].empty?
        raise Error.new("A --path is required") if settings[:path].nil? || settings[:path].empty?
        
        raise Error.new("A local --output directory is required") if settings[:output].nil? || settings[:output].empty?
        raise Error.new("Output directory <#{settings[:output]}> exists but is not a directory") if File.exist?(settings[:output]) && !File.directory?(settings[:output])
        
        true
      end

      # Logs what's about to happen.
      def before_sync
        super()
        user_msg = settings[:username] ? "#{settings[:username]}@" : ''
        log.info("Mirroring #{settings[:protocol]} #{user_msg}#{settings[:host]}:#{port}#{settings[:path]}")
      end
      
      # Perform the sync.
      #
      # If the `dry_run` setting was given, will just output the
      # `lftp` command-line that would have been run.  Otherwise will
      # execute the command-line in a subprocess and log its output at
      # the `DEBUG` level.
      def sync
        command = send("#{settings[:protocol]}_command")
        if settings[:dry_run]
          log.info(command)
        else
          started_at = Time.now
          IO.popen(command).each { |line| handle_output(line) }
          self.duration = (Time.now - started_at)
        end
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
        command << "mirror --verbose=#{VERBOSITY} #{settings[:path]} #{settings[:output]};"
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

      private

      # :nodoc:
      def handle_output line
        log.debug(line.chomp)
      end

    end
  end
end
