module Wukong
  module Load

    # Syncs a "live" input directory, with possibly growing files,
    # with an output directory or directories.  In the case of
    # multiple output directories, useful to get higher throughput,
    # consecutive files will be placed round-robin at the same
    # relative paths into each output directory: *merging* the output
    # directories downstream should yield the same result as having
    # just used a single output directory.
    #
    # Files in the output directories will only appear when files in
    # the input directory stop growing.
    #
    # By default, files in the output directories will be hardlinks at
    # the same relative paths as their corresponding (complete) files
    # in the input directory.
    #
    # A second mode of operation (activated using the `split` option
    # in #initialize) will split all (complete) files in the input
    # directory into equal-sized, smaller files in the output
    # directories with the same relative path as the original file in
    # the input directory but with numerically increasaing suffixes.
    # Size and splitting behavior are both configurable.
    #
    # In both of these modes of operation, two additional options are
    # available:
    #
    # 1. Instead of placing files (or splits of a file) at the same
    # relative path in the output directories as in the input
    # directory, a complete ordering on the files, based on the time
    # of syncing and the file's original relative path, can be used
    # instead.  This is a good choice when downstream consumers
    # (e.g. - Storm) need ordered input.  This option is activated
    # using the `ordered` option in #initialize.  When using multiple
    # output directories, ordering is maintained **across**
    # directories so that the downstream merge of multiple, ordered
    # output directories is itself orderered.
    #
    # 2. In addition to creating the output file (or splits) a JSON
    # metadata file can also be created for each input file.  This
    # file contains MD5 and size information and a pointer to the
    # actual output file it corresponds to.  Metadata files live in a
    # different directory tree than the original files themselves but
    # otherwise have the same relative paths, except with a `.meta`
    # extension.  This means that metadata files are always
    # lexicographically later in sort order which means they'll be the
    # last files to be synced to some other filesystem (like S3).
    # Metadata files are also small which means they are fast to
    # transfer and downstream consumers are unlikely to encounter them
    # in a partial state.  The combination of these factors means that
    # a downstream consumer can use the presence or absence of a
    # metadata file as an indicator of whether the **actual** output
    # file (or splits) has finished arriving.  When creating multiple
    # output directories, metadata files are created within each
    # output directory separately (but still in a way consistent with
    # them being merged downstream).
    class PrepareSyncer < Syncer
      
      include Logging
      include UsesFileState
      
      autoload :Handler,           'wukong-load/syncers/prepare_syncer/handler'
      autoload :SplittingHandler,  'wukong-load/syncers/prepare_syncer/splitting_handler'

      # Construct a new PrepareSyncer from the given `settings` for
      # the given `source` with the given `name`.
      #
      # @return [PrepareSyncer]
      def self.from_source settings, source, name
        raise Error.new("An --input directory is required") unless settings[:input]
        raise Error.new("At least one --output directory is required") if (settings[:output].nil? || settings[:output].empty?)
        new(settings.dup.merge({
          name:   name.to_s,
          input:  File.join(settings[:input], name.to_s),
          output: settings[:output].map { |dir| File.join(dir, name.to_s) },
        }).merge(source[:prepare] || {}))
      end
      
      # Configure the `settings` for use with an PrepareSyncer.
      #
      # @param [Configliere::Param] settings
      def self.configure settings
        settings.define :input,   description: "Input directory of (possibly growing) files"
        settings.define :output,  description: "Comma-separated list of output directories", default: [], type: Array
        
        settings.define :ordered,  description: "Create a total ordering within the output directory", default: false, type: :boolean
        settings.define :metadata, description: "Create a metadata file for each file in the output directory", default: false, type: :boolean
        
        settings.define :split,   description: "Split each (complete) input file into several files each with a certain number of lines", default: false, type: :boolean
        settings.define :lines,   description: "Split into files of this many lines", type: Integer, default: 10_000
        settings.define :bytes,   description: "Split into files of this many bytes instead of splitting by line", type: Integer
        settings.define :split_program, description: "Path to the `split` program", default: 'split'
        
        settings.description = <<-EOF
Syncs an --input directory, with possibly growing files, to an
--output directory or directories.

Files will only appear in the --output directory when they stop
growing in the --input directory.  At least two invocations of wu-sync
(with identical parameters) are therefore necessary in order to create
files in the --output directory.  Files in the --input directory which
don't change size between invocations are considered "complete" and
will be created in the --output directory:

  $ wu-sync prepare --input=/var/inbound --output=/data/outbound
  $ wu-sync prepare --input=/var/inbound --output=/data/outbound

The default behavior is to mirror exactly the structure of the --input
directory in the --output directory.

The --split option will split each (complete) file in the --input
directory into several files in the --output directory.  This is
useful for enforcing a maximum individual file size for downstream
consumers.  The default behavior is to split after every 10,000 lines
but this can be changed with the --lines option or the --bytes option,
which will split after a certain number of bytes instead of lines.

  $ wu-sync prepare --input=/var/ftp/inbound --output=/data/ftp/outbound --split --lines=100_000
  $ wu-sync prepare --input=/var/ftp/inbound --output=/data/ftp/outbound --split --bytes=1_048_576

The --ordered option can be used to create a complete ordering of
files in the --output directories, useful for when downstream
consumers (e.g. - Storm) require ordered input.

The --metadata option will create a JSON metadata file in the output
directories in addition to each output file.

In situations where the input file and output file are in a one-to-one
correspondence (without the --split option), files in the --output
directories will be hardlinks pointing at their equivalent files in
the --input directory.

Multiple --output directories (usually on different hard drives) can
dramatically speed up operations:

  $ wu-sync prepare --input=/var/ftp/inbound --output=/data/ftp/outbound_1,/data/ftp/outbound_2,/data/ftp/outbound_3 --split --lines=100_000
EOF
      end

      # Validate the `settings` for this prepare syncer.
      #
      # @raise [Wukong::Error] if the local `input` directory is missing or not a directory
      # @raise [Wukong::Error] if the local `output` directory eixsts but is not a directory
      # @return [true]
      def validate
        raise Error.new("A local --input directory is required") if settings[:input].nil? || settings[:input].empty?
        raise Error.new("Input directory <#{settings[:input]}> does not exist") unless File.exist?(settings[:input])
        raise Error.new("Input directory <#{settings[:input]}> is not a directory") unless File.directory?(settings[:input])
        
        raise Error.new("At least one --output directory is required") if (settings[:output].nil? || settings[:output].empty?)
        settings[:output].each do |dir|
          raise Error.new("Output directory <#{dir}> exists but is not a directory") if File.exist?(dir) && !File.directory?(dir)
        end
        true
      end

      # A Handler class that will actually make the links to newly
      # static files.
      attr_accessor :handler

      # A list of files that was processed during this invocation.
      attr_accessor :files

      # Tracks counts of files examined, new, ignored, in error, and
      # processed.
      attr_accessor :counts
      
      # Did all input files process?
      #
      # @return [true, false]
      def success?
        self.counts[:examined] == 0 || self.counts[:error] == 0
      end
      
      # Did any of the input files fail to process?
      #
      # @return [true, false]
      def failed?
        self.counts[:error] > 0
      end

      # The absolute path to the input directory.
      #
      # @return [Pathname]
      def absolute_input_directory
        Pathname.new(File.absolute_path(settings[:input]))
      end

      # The absolute path to the output directories.
      #
      # @return [Array<Pathname>]
      def absolute_output_directories
        settings[:output].map do |dir|
          Pathname.new(File.absolute_path(dir))
        end
      end

      # Setup this PrepareSyncer by loading any file size state that's
      # already present.
      def setup
        super()
        load_file_state
        create_handler
        self.files  = []
        self.counts = { examined: 0, new: 0, processed: 0, ignored: 0, error: 0 }
      end

      # Logs a message.
      def before_sync
        super()
        log.info("Archiving <#{settings[:input]}> to <#{settings[:output]}>")
      end

      # Logs a message.
      def after_sync
        super()
        log.debug("#{settings[:input]}: #{self.counts[:examined]} files, #{self.counts[:new]} new, #{self.counts[:processed]} processed, #{self.counts[:ignored]} ignored, #{self.counts[:error]} error")
      end

      # Perform the sync.
      #
      # If the `dry_run` setting was given, will not create any files
      # in the `output` directory nor update the current state of file
      # sizes in the `input` directory, but will log a message at the
      # `DEBUG` level for each file it would have processed.
      def sync
        started_at = Time.now
        absolute_input_directory.find do |path|
          next if path.directory?
          self.counts[:examined] += 1
          if already_processed?(path)
            self.counts[:ignored] += 1
            next
          end
          same_size?(path) ? process!(path) : remember_size!(path)
        end
        self.duration = (Time.now - started_at)
      ensure
        save_file_state!
      end

      # Has the given `path` already been processed?
      #
      # @param [Pathname] path
      # @return [true, false]
      def already_processed? path
        file_state[path.to_s] == true
      end

      # Is the given `path` the same size as it was on the last
      # invocation?
      #
      # @param [Pathname] path
      # @return [true, false]
      def same_size?(path)
        return false unless existing_size = file_state[path.to_s]
        existing_size == path.size
      end

      # Process the complete file at  `path`.
      #
      # Delegates to the #handler.
      #
      # @param [Pathname] path
      # @see Handler#process
      def process! path
        if handler.process(path)
          self.counts[:processed] += 1
          self.files << handler.fragment_for(path)
          file_state[path.to_s] = true
        else
          self.counts[:error] += 1
        end
      end

      # Remember the size of the file at `path` for the next
      # invocation.
      #
      # @param [Pathname] path
      def remember_size!(path)
        self.counts[:new] += 1
        file_state[path.to_s] = path.size
      end

      protected

      # Creates the handler for this syncer.
      #
      # @return [Handler]
      def create_handler
        settings[:ordered] = true if settings[:metadata]
        handler_settings   = settings.dup.merge(input: absolute_input_directory, output: absolute_output_directories)
        self.handler       = (settings[:split] ? SplittingHandler : Handler).new(self, handler_settings)
      end

      # The basename for the file used to store state between invocations.
      #
      # Uses the #name of the prepare syncer if present so that
      # multiple named syncers can operate in parallel.
      #
      # @return [String]
      def file_state_basename
        "sync-prepare#{'-' + name if name}.json"
      end
      
    end
  end
end
