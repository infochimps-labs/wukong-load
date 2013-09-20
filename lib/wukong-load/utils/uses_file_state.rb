module Wukong
  module Load
    module UsesFileState

      attr_accessor :file_state
      
      def file_state_dir
        Dir.tmpdir
      end

      def file_state_path
        File.join(file_state_dir.to_s, file_state_basename)
      end

      def file_state_basename
        self.class.to_s.split('::').last + '.json'
      end

      def file_state_exists?
        File.exists?(file_state_path)
      end

      def ignore_file_state?
        settings[:restart]
      end

      def load_file_state
        if ignore_file_state?
          if file_state_exists?
            log.debug("Ignoring existing state at <#{file_state_path}>")
          else
            log.debug("Ignoring (non-existent) state at <#{file_state_path}>")
          end
          self.file_state = {}
        else
          if file_state_exists?
            log.debug("Loading existing state from <#{file_state_path}>")
            self.file_state = MultiJson.load(File.read(file_state_path))
          else
            log.debug("No prior state at <#{file_state_path}>")
            self.file_state = {}
          end
        end
      end

      def save_file_state!
        log.debug "Saving state to <#{file_state_path}>"
        File.open(file_state_path, "w") { |f| f.write(MultiJson.dump(file_state)) } unless settings[:dry_run]
      rescue => e
        raise Wukong::Error.new("Couldn't save state to <#{file_state_path}>: #{e.class} -- #{e.message}")
      end
      
    end
  end
end
