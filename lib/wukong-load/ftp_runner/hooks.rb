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
        # @param [Array<String>] newly_downloaded_paths the remote paths on the server that were newly downloaded
        def after_each source, newly_downloaded_paths
        end

        # Run upon an error during processing a source.
        #
        # @param [Wukong::Load;:FTP::FTPSource] source
        # @param [Exception] error the error that occurred
        def on_error source, error
        end
        
      end
    end
  end
end

