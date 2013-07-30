require_relative("default_file_handler")

module Wukong
  module Load
    module FTP
      class FTPSource

        include Logging

        PROTOCOLS = {
          'ftp'  => 21,
          'ftps' => 443,
          'sftp' => 22
        }.freeze

        VERBOSITY = 3
        
        attr_accessor :settings

        def initialize settings
          self.settings = settings
        end
        
        def validate
          raise Error.new("Unsupported --protocol: <#{settings[:protocol]}>") unless PROTOCOLS.include?(settings[:protocol])
          raise Error.new("A --host is required") if settings[:host].nil? || settings[:host].empty?
          raise Error.new("A --path is required") if settings[:path].nil? || settings[:path].empty?
          raise Error.new("A local --output directory is required") if settings[:output].nil? || settings[:output].empty?
          raise Error.new("A local --links directory is required")  if settings[:links].nil? || settings[:links].empty?
          raise Error.new("The --name of a directory within the output directory is required") if settings[:name].nil? || settings[:name].empty?
        end

        def port
          settings[:port] || PROTOCOLS[settings[:protocol]]
        end

        def mirror
          user_msg = settings[:username] ? "#{settings[:username]}@" : ''
          log.info("Mirroring #{settings[:protocol]} #{user_msg}#{settings[:host]}:#{port}#{settings[:path]}")
          command = send("#{settings[:protocol]}_command")
          if settings[:dry_run]
            log.info(command)
            []
          else
            subprocess = IO.popen(command)
            handle_output(subprocess.readlines)
          end
        end

        def newly_downloaded_paths output
          new_file_lines_include = "Transferring file"
          output.find_all do |line|
            line.include?(new_file_lines_include)
          end.map do |line|
            line =~ /`(.*)'/
            $1
          end
        end
        
        def handle_output output
          paths_processed = []
          output.each { |line| log.debug(line.chomp) }
          newly_downloaded_paths(output).each_with_index do |path, index|
            file_handler.process(path, index)
            paths_processed << path
          end
          paths_processed
        end

        def file_handler
          @file_handler ||= FTPFileHandler.new(settings)
        end

        def ftp_command
          lftp_command
        end

        def ftps_command
          lftp_command('set ftps:initial-prot "";', 'set ftp:ssl-force true;', 'set ftp:ssl-protect-data true;')
        end
        
        def sftp_command
          lftp_command
        end

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

        def lftp_program
          settings[:lftp_program] || 'lftp'
        end
        
      end
    end
  end
end

