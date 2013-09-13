module Wukong
  module Load
    module FTP
      
      class MirroredFiles < Hash

        attr_accessor :name

        def initialize name
          super()
          self.name = name
        end

        module Overridable
          def dir
            Dir.tmpdir
          end
        end
        include Overridable
        
        def path
          File.join(dir.to_s, basename)
        end

        def file_exists?
          File.exists?(path)
        end

        def load
          return unless file_exists?
          new_map = MultiJson.load(File.read(path))
          self.clear
          self.merge!(new_map)
        end
        alias_method :reload, :load

        def save
          File.open(path, 'w') { |f| f.write(MultiJson.dump(self)) }
        end
        
        def basename
          self.class.to_s.gsub('::', '_') + "-#{name}.json"
        end
        
      end
    end
  end
end
