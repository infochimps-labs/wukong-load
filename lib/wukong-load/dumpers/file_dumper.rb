require 'shellwords'
module Wukong
  module Load
    
    class FileDumper < Dumper

      include Logging

      def self.validate settings
        raise Error.new("Must specify an --input path") if settings[:input].nil? || settings[:input].empty?
        raise Error.new("Input path <#{settings[:input]}> does not exist") unless File.exist?(settings[:input])
        raise Error.new("Input path <#{settings[:input]}> is not a file")  unless File.file?(settings[:input])
      end

      def self.configure settings
        settings.define(:input,          description: "Path to the input file")
        settings.define(:clean,          description: "Run a cleaning process on dumped files to normalize line-endings", default: true, type: :boolean)
        settings.define(:decorate,       description: "Decorate each line of the output with the filename and line number of the input file", default: true, type: :boolean)
        settings.define(:delimiter,      description: "Delimiter used in output when decorating", default: "\t")
        settings.description = <<-EOF
Dump data from a file to STDOUT:

  $ wu-dump file --input=/path/to/file.txt
  content of the first line
  content of the second line
  content of the third line
  ...

For normal files, this mode of operation not very interesting and
functions just like `cat`.  Some differences do exist:

  - The input file will have its line-endings normalized (set
    --clean=false) to disable this.

  - Compressed files (`.gz`, `.bz2`) will be decompressed as they are
    read so

      $ wu-dump file --input=/path/to/file.gz
      $ wu-dump file --input=/path/to/file.bz2

    both work as expected.

  - Archived files (`.tar.gz`, `.tar.bz2`, `.zip`) will be expanded
    and each enclosed file will be dumped separately, so

      $ wu-dump file --input=/path/to/file.tar.gz
      $ wu-dump file --input=/path/to/file.tar.bz2
      $ wu-dump file --input=/path/to/file.zip

    all work as expected.

The --decorate option will decorate each line of the dumped output
with the filename and line number of the line from the input file:

  $ wu-dump file --input=/path/to/file.txt --decorate
  /path/to/file.txt	1	content of the first line
  /path/to/file.txt	2	content of the second line
  /path/to/file.txt	3	content of the third line
  ...

The default field delimiter is a tab but can be changed with the
--delimiter option.

When dumping compressed files with --decorate, the line number in the
outputwill correspond to the line number of the uncompressed output.

When dumping archived files with --decorate, the path to the input
file will be the path to the archive file and the line number will be
the consecutive line number across **all** files in the archive.
Archives with a single file are therefore simplest for downstream code
to process.
EOF
      end

      def dump
        before_dump
        system dump_command
        after_dump
      rescue => e
        on_error(e)
        raise e
      end

      def before_dump
        log.debug("Dumping <#{path}>")
      end

      def after_dump
      end

      def on_error error
        log.error(error)
      end

      def dump_command
        case
        when settings[:clean] && settings[:decorate]
          "#{cat_command} | #{clean_command} | #{awk_command}"
        when settings[:clean]
          "#{cat_command} | #{clean_command}"
        else
          "#{cat_command}"
        end
      end

      def path
        Shellwords.escape(settings[:input].to_s)
      end

      def cat_command
        "#{cat_program} #{path}"
      end

      def clean_command
        "tr -d '\r' | tr '\r' '\n'"
      end
      
      def awk_command
        %Q{awk '{ print "#{path}#{settings[:delimiter]}" FNR "#{settings[:delimiter]}" $0 }'}
      end

      def cat_program
        case settings[:input]
        when /\.tar\.gz$/, /\.tgz$/   then "tar -xzO -f"
        when /\.tar\.bz2$/, /\.tbz2$/ then "tar -xjO -f"
        when /\.gz$/                  then "zcat"
        when /\.bz2$/                 then "bzcat"
        when /\.zip$/                 then "unzip -p"
        else
          "cat"
        end
      end
    end
    
  end
end
