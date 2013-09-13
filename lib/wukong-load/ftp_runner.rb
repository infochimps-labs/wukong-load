require_relative("ftp_runner/ftp_source")
require_relative("ftp_runner/hooks")

module Wukong
  module Load

    # Runs the wu-ftp command.
    #
    # Most of the complexity is contained in the construction of the
    # #sources Hash.  The FTP::FTPSource does most of the
    # heavy-lifting.
    class FTPRunner < Wukong::Runner

      include FTP
      include FTP::Hooks

      FILESIZE_MAP_FILE = "./tmp/filesize_map.json"
      LOCKFILE = "./ftp_runner.rb.pid"

      usage "[SOURCE]"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-ftp is a tool for transferring data from an FTP/FTPS/SFTP
        server to local disk with a directory structure that is
        compatible for downstream consumption by tools like Storm.

        Here's an example of pulling down some Twitter data from an
        FTP server to the local `/tmp/raw` directory, with
        lexicographically ordered hardlinks in the local `/tmp/clean`
        directory using the data type `tweets`:

          $ wu-ftp --host=ftp.example.com --output=/tmp/raw --name=tweets --links=/tmp/clean

        The protocol, host, and credentials can all be changed with
        flags.

        If the `--ftp_sources` setting is given, all sources within
        this Hash will be downloaded when `wu-ftp` is invoked.
        Alternatively, when `--ftp_sources` is present, a single
        source can be invoked by name:

          $ wu-ftp --output=/tmp/raw --links=/tmp/clean source_name

        `wu-ftp` requires the `lftp` program in order to function.
      EOF
      
      include Logging

      # Delegates to each FTP source to validate itself.
      #
      # @see FTP::FTPSource#validate
      # @return [true]
      # @raise [Wukong::Error] if validation fails
      def validate
        sources.each_value(&:validate)
        true
      end

      # Iterates through and mirrors each FTP source, handling hooks
      # and errors.
      def run
        abort("ERROR: lockfile exists") unless create_lockfile
        begin
          sources.each_pair do |name, source|
            begin
              before_each(source)
              paths_processed = source.mirror
              after_each(source, paths_processed)
            rescue Wukong::Error => e
              log.error(e)
              on_error(source, e)
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

      def after_each source, paths_processed
        filesize_map = {}
        if File.exists?(FILESIZE_MAP_FILE)
          File.open(FILESIZE_MAP_FILE, "r") do |f|
            log.debug "reading from #{FILESIZE_MAP_FILE}"
            filesize_map = MultiJson.load(f)
          end
        end

        # process each finished file in the filesize_map
        filesize_map.keys.each do |filename|
          if (source.finished?(filename, paths_processed))
            source.handle_finished_file filename
            filesize_map.delete filename
          end
        end

        paths_processed.each do |filename|
          path = "#{settings[:output]}/#{settings[:name]}/#{filename}"
          filesize_map[filename] = File.stat(path).size if File.exists?(path)
        end

        log.debug "filesize_map is #{filesize_map.inspect}"
        File.open(FILESIZE_MAP_FILE, "w") do |f|
          log.debug "writing to #{FILESIZE_MAP_FILE}"
          f.write(MultiJson.dump(filesize_map))
        end
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

    end
  end
end
