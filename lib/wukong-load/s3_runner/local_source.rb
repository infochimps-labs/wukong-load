module Wukong
  module Load
    module S3
      class LocalSource

        include Logging

        attr_accessor :settings

        def initialize settings
          self.settings = settings
        end
        
        def validate
          raise Error.new("A local --input directory is required") if settings[:input].nil? || settings[:input].empty?
          raise Error.new("An S3 --bucket is required")  if settings[:bucket].nil? || settings[:bucket].empty?
          raise Error.new("The --name of a directory within the input directory is required") if settings[:name].nil? || settings[:name].empty?
        end

        def archive
          log.info("Archiving #{settings[:name]} from #{settings[:input]} to #{settings[:bucket]}")
          if settings[:dry_run]
            log.info(sync_command)
            []
          else
            subprocess = IO.popen(sync_command)
            subprocess.each do |line|
              handle_output(line)
            end
          end
        end

        def handle_output line
          log.debug(line.chomp)
        end
        
        def sync_command
          src  = File.join(File.expand_path(settings[:input].to_s), settings[:name].to_s, "")  # always use a trailing '/' with s3cmd
          raise Error.new("Input directory <#{src}> does not exist") unless File.exist?(src)
          raise Error.new("Input directory <#{src}> is not a directory") unless File.directory?(src)
          
          dest = File.join(settings[:bucket] =~ %r{^s3://} ? settings[:bucket].to_s : "s3://#{settings[:bucket]}", settings[:name].to_s + '/')

          if settings[:s3cmd_config]
            raise Error.new("s3cmd config file <#{settings[:s3cmd_config]}> does not exist") unless File.exist?(settings[:s3cmd_config])
            config_file = "--config=#{settings[:s3cmd_config]}"
          else
            config_file = ""
          end

          "#{s3cmd_program} sync #{src} #{dest} --no-delete-removed --bucket-location=#{settings[:bucket_location]} #{config_file}"
        end

        def s3cmd_program
          settings[:s3cmd_program] || 's3cmd'
        end
        
      end
    end
  end
end

