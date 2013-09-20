module Wukong
  module Load
    class PrepareSyncer

      # Can be included into another Handler class to make that
      # handler create a strict ordering for files in its output
      # directory.
      module OrderedHandler
        
        # Counter for keeping track of the number of files processed.
        #
        # Defaults to 0.
        #
        # @return [Integer]
        def counter
          @counter ||= 0
        end
        attr_writer :counter
        
        # Increments the #counter.
        #
        # @param [Pathname] original
        def after_process original
          super(original)
          self.counter += 1
        end
        
        # Return the output path for the given `original` file.
        #
        # @param [Pathname] original
        # @param [Time] time use this specific time instead of the current UTC time
        # @return [Pathname]
        def path_for original, time=nil
          time ||= Time.now.utc
          settings[:output].join(daily_directory_for(time, original)).join(slug_for(time, original))
        end

        # Return the daily directory for the given `time`.
        #
        # @param [Time] time
        # @return [String]
        def daily_directory_for time, original
          File.join(top_level_of(original, settings[:input]), time.strftime("%Y/%m/%d"))
        end

        # Return the basename to use for the given `time` for given
        # `original` file.
        def slug_for(time, original)
          [
           time.strftime("%Y%m%d-%H%M%S"),
           counter.to_s,
           relative_path_of(original, settings[:input]).to_s.gsub(%r{/},'-'),
          ].join('-')
        end
        
      end
    end
  end
end

