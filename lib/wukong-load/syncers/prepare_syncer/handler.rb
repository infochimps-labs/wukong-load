require_relative('metadata_handler')
module Wukong
  module Load
    class PrepareSyncer

      autoload :OrderedHandler, 'wukong-load/syncers/prepare_syncer/ordered_handler'

      # Base class for other handlers to subclass.
      class Handler

        # The PrepareSyncer this Handler is for.
        attr_accessor :syncer

        # Settings this handler was created with, probably inherited
        # from the PrepareSyncer that created it.
        attr_accessor :settings

        include Logging
        include MetadataHandler

        # Create a new Handler with the given `settings`.
        #
        # @param [PrepareSyncer] syncer the syncer this handler is for
        # @param [Configliere::Param] settings
        # @option settings [Pathname] :input the input directory
        # @option settings [Pathname] :output the output directory
        # @option settings [true, false] :dry_run log what would be done instead of doing it
        # @option settings [true, false] :ordered create totally ordered output
        # @option settings [true, false] :metadata create metadata files for each output file
        def initialize syncer, settings
          self.syncer   = syncer
          self.settings = settings
          extend (settings[:dry_run] ? FileUtils::NoWrite : FileUtils)
          extend OrderedHandler if settings[:ordered]
        end

        # Process the `original` file in the input directory.
        #
        # @param [Pathname] original
        def process original
          before_process(original)
          process_input(original)
          after_process(original)
          true
        rescue => e
          on_error(original, e)
          false
        end

        # Creates a hardlink in the `output` directory with the same
        # relative path as `path` in the input directory.
        #
        # @param [Pathname] original
        def process_input original
          create_hardlink(original, path_for(original))
        end

        module Hooks
          # Run before processing each file.
          #
          # @param [Pathname] original the original file in the input directory
          def before_process original
          end
          
          # Run after successfully processing each file.
          #
          # @param [Pathname] original the original file in the input directoryw
          def after_process original
          end
          
          # Run upon an error during processing.
          #
          # @param [Error] error
          # @param [Pathname] original the original file in the input directoryw
          def on_error original, error
            log.error("Could not process <#{original}>: #{error.class} -- #{error.message}")
          end
        end
        include Hooks
        
        # Creates a hardlink at `copy` pointing to `original`.
        #
        # @param [Pathname] original
        # @param [Pathname] copy
        def create_hardlink original, copy
          mkdir_p(copy.dirname)
          log.debug("Linking #{copy} -> #{original}")
          ln(original, copy, force: true)
          process_metadata_for(copy) if settings[:metadata]
        end

        # Return a path in the `output` directory that has the same
        # relative path as `original` does in the input directory.
        #
        # @param [Pathname] original
        # @return [Pathname]
        def path_for original
          relative_path = original.relative_path_from(settings[:input])
          settings[:output].join(relative_path)
        end
        
        # Return the path of `original` relative to the containing
        # `dir`.
        #
        # @param [Pathname] original
        # @param [Pathname] dir
        # @return [Pathname]
        def relative_path_of original, dir
          original.relative_path_from(dir)
        end

        # Returns the top-level directory of the `original` path,
        # relative to `dir`.
        #
        # If the `original` is in `dir` itself, and not a
        # subdirectory, returns the string "root".
        #
        # @param [Pathname] original
        # @param [Pathname] dir
        # @return [String, "root"]
        def top_level_of(original, dir)
          top_level, rest = relative_path_of(original, dir).to_s.split('/', 2)
          rest ? top_level : 'root'
        end
        
      end
    end
  end
end

