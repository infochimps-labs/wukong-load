module Wukong
  module Load
    module UsesLockfile

      def setup
        super()
        require 'tmpdir'
        lockfile_exists! if lockfile_exists?
      end

      def lockfile_dir
        Dir.tmpdir
      end

      def lockfile
        File.join(lockfile_dir.to_s, lockfile_basename)
      end

      def lockfile_basename
        self.class.to_s.gsub('::', '_') + '.pid'
      end

      def lockfile_exists?
        File.exists?(lockfile)
      end

      def lockfile_exists!
        raise Wukong::Error.new("Lockfile #{lockfile} exists!  Aborting...")
      end
      
      def create_lockfile!
        lockfile_exists! if lockfile_exists?
        File.open(lockfile, "w") do |f|
          pid = Process.pid.to_s
          log.debug "Writing PID #{pid} to lockfile #{lockfile}"
          f.write(pid)
        end
      rescue => e
        raise Wukong::Error.new("Couldn't create #{lockfile}: #{e.class} -- #{e.message}")
      end

      def delete_lockfile!
        log.debug "Deleting lockfile #{lockfile}"
        File.delete(lockfile)
      end
      
    end
  end
end
