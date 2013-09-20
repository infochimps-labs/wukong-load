module Wukong
  module Load

    # A base class for all syncers to subclass.
    class Syncer

      include Logging

      # Settings for this syncer.
      attr_accessor :settings

      # Name of this syncer.
      attr_accessor :name

      # Construct a new Syncer from the given `settings` for the given
      # `source` with the given `name`.
      #
      # @return [Syncer]
      def self.from_source settings, source, name
        raise NotImplementedError.new("Override the #{self}.from_source method")
      end

      # Create a new syncer.
      # 
      # @param [Configliere::Param] settings
      def initialize settings
        self.settings = settings
        self.name     = settings[:name]
      end

      # Configure the given `settings` with more specific options.
      #
      # @param [Configliere::Param] settings
      def self.configure settings
      end

      # Run this syncer, performing validation, setting up, sycning,
      # and cleaning up.
      def run
        validate
        setup
        before_sync
        sync
        after_sync
      rescue => e
        on_error(e)
        raise e
      end
      
      # Validate this syncer.
      #
      # @return [true]
      def validate
        true
      end

      # Setup this syncer.
      def setup
      end

      # Perform the sync.
      def sync
        raise NotImplementedError.new("Override the #{self.class}#sync method")
      end

      module Hooks
        # Run before a sync.
        def before_sync
        end
        
        # Run after a successful sync.
        def after_sync
        end
        
        # Run when a sync fails for some reason.
        #
        # @param [Error] error the error that was thrown
        def on_error error
          log.error(error)
        end
      end
      include Hooks

    end
  end
end

  
