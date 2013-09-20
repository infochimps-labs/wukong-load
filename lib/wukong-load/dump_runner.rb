module Wukong
  module Load

    autoload :KafkaDumper,      'wukong-load/dumpers/kafka_dumper'
    autoload :FileDumper,       'wukong-load/dumpers/file_dumper'
    autoload :DirectoryDumper,  'wukong-load/dumpers/directory_dumper'
    
    # Implements the `wu-dump` command.
    #
    # Several parts of the boot sequence for a Wukong runner are
    # utilized here.
    #
    # @see Wukong::Runner::BootSequence
    class DumpRunner < Wukong::Runner

      include Logging
      
      usage "DATA_STORE"

      description <<-EOF
wu-dump is a tool for dumping data from multiple, pluggable data
stores, to STDOUT, including:

  kafka
  file
  directory
  elasticsearch (planned)
  mongodb (planned)
  hbase (planned)
  mysql (planned)

For more help on a specific dumper, run:

  $ wu-dump DUMPER_TYPE --help
EOF

      # Asks the dumper klass (if given) to load any necessary code.
      def load
        super()
        dumper_klass.load if dumper_klass
      end

      # Asks the dumper klass (if given) to configure any settings.
      def configure
        super()
        dumper_klass.configure(settings) if dumper_klass
      end

      # Asks the dumper klass (if given) to perform any setup.
      def setup
        super()
        dumper.setup if dumper_klass
      end

      # Ensure that we were passed a data store name that we know
      # about.
      #
      # @raise [Wukong::Error] if the data store is missing or unknown
      # @return [true]
      def validate
        case
        when data_store_name.nil?
          raise Error.new("Must provide the name of a data store as the first argument")
        when dumper_klass.nil?
          raise Error.new("No dumper defined for data store <#{data_store_name}>")
        end
        dumper_klass.validate(settings)
        true
      end
      
      # Dump the data that was requested.
      def run
        dumper.dump
      end

      # The name of the data store to dump.
      #
      # @return [String]
      def data_store_name
        ARGV.detect { |arg| arg !~ /^--/ }
      end
      
      # The dumper class to use as determined by the
      # `#data_store_name`.
      #
      # @return [Class, nil] the dumper class or `nil` if no such dumper exists
      # @see #data_store_name
      # @see #dumper
      def dumper_klass
        case data_store_name.to_s.downcase
        when 'kafka'      then KafkaDumper
        when 'file'       then FileDumper
        when 'directory'  then DirectoryDumper
        end
      end

      # The created dumper instance.
      #
      # Determined dynamically from the `#dumper_klass` and the
      # `#settings`.
      #
      # @return [Dumper]
      # @see #dumper_klass
      def dumper
        @dumper ||= dumper_klass.new(settings)
      end
    end
  end
end
