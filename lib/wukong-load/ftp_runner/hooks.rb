module Wukong
  module Load
    module FTP

      # Contains methods designed to be overridden by libraries and
      # user-code.
      #
      # @see Wukong::Load::FTPRunner#run
      module Hooks

        # Run before each source is mirrored.
        #
        # @param [Wukong::Load::FTP::FTPSource] source
        def before_each source
        end

        # Run after each source is successfully mirrored.
        #
        # @param [Wukong::Load::FTP::FTPSource] source
        def after_each source
          source.handle_newly_mirrored_files
        end

        # Run upon an error during processing a source.
        #
        # @param [Wukong::Load;:FTP::FTPSource] source
        # @param [Exception] error the error that occurred
        def on_error source, error
          log.error("Could not process source <#{source.settings[:name]}>: #{e.class} -- #{e.message}")
          error.backtrace.each { |line| log.debug(line) }
        end
        
      end
    end
  end
end

