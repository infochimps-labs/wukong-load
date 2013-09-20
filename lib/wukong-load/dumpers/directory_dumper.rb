module Wukong
  module Load
    
    class DirectoryDumper < Dumper

      include Logging
      include UsesLockfile
      include UsesFileState

      def self.load
        require 'find'
      end

      def self.validate settings
        raise Error.new("Must specify an --input directory") if settings[:input].nil? || settings[:input].empty?
        raise Error.new("Input directory <#{settings[:input]}> does not exist")     unless File.exist?(settings[:input])
        raise Error.new("Input directory <#{settings[:input]}> is not a directory") unless File.directory?(settings[:input])
      end

      def self.configure settings
        settings.define(:input,          description: "Directory to dump")
        settings.define(:delimiter,      description: "Delimiter used in output", default: "\t")
        settings.define(:parallelism,    description: "Maximum number of files processed in parallel", default: 10, type: Integer)
        settings.define(:restart,        description: "Restart, re-reading any files which may already have been dumped", default: false, type: :boolean)
        settings.define(:ignore,         description: "Ignore files matching this regular expression", type: Regexp)
        settings.define(:only,           description: "Only dump files matching this regular expression", type: Regexp)
        settings.define(:name,           description: "Meaningful name for the directory")
        settings.description = <<-EOF
Idempotently dump data from files in a directory to STDOUT in
tab-separated format:

  $ wu-dump directory --input=/path/to/dir
  /path/to/dir/file1.txt	1	content of the first line
  /path/to/dir/file1.txt	2	content of the second line
  /path/to/dir/file2.txt	1	content of the first line
  ...

The first field is the path to a file within the input directory, the
second is the content of each line of each such file, and the third is
the line number of each line.

Compressed files (`.gz`, `.bz2`) will be as they are read.  Line
numbers refer to the uncompressed line number.

Archived files (`.tar.gz`, `.tar.bz2`, `.zip`) will be expanded as
they are read.  The path to the file will be the path to the archive
file and the line number will be the line number across all files in
the archive.  Archives with a single file are therefore simplest for
downstream code to process.

Files within the directory are processed in parallel using
--parallelism simultaneous threads.

On repeated invocations, files that were already dumped will *not* be
dumped again.  This feature allows repeatedly invoking `wu-dump
directory` on the same --input directory only picking up incremental
changes.  Use the --restart option (or delete the file state) to force
dumping of all the data in the input directory.

The --ignore and --only options can be used for more fine-grained
control over which files are dumped.
EOF
      end

      attr_accessor :paths

      def initialize settings
        super(settings)
        self.paths = []
      end

      def setup
        super()
        lockfile_exists! if lockfile_exists?
        load_file_state
      end

      def file_state_basename
        tmpfile_basename + '.json'
      end
      
      def lockfile_basename
        tmpfile_basename + '.lock'
      end

      def tmpfile_basename
        'dump-dir'
      end

      def dump
        create_lockfile!
        find_paths_to_dump
        dump_all_paths
      ensure
        save_file_state!
        delete_lockfile!
      end

      def find_paths_to_dump
        Find.find(settings[:input]) do |path|
          next if File.directory?(path)
          next if settings[:ignore] && path =~ settings[:ignore]
          next if settings[:only]   && path !~ settings[:only]
          if already_dumped?(path)
            log.debug("Already dumped <#{path}>")
            next
          end
          self.paths << path
        end
      end

      def dump_all_paths
        EM.run do
          EventMachine::Iterator.new(paths, settings[:parallelism]).each(
          proc { |path, iter| dump_path(path) ; iter.next},
          proc { |responses| EM.stop })
        end
      end

      def already_dumped?(path)
        file_state.include?(path)
      end

      def dump_path path
        FileDumper.new(settings.merge(input: path)).dump
        file_state[path] = true
      end

    end
  end
end
