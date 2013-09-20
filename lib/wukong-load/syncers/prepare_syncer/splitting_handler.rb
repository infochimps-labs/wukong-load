require 'wukong-load/dumpers/file_dumper'
module Wukong
  module Load
    class PrepareSyncer

      # When syncing, splits each input file into several output files
      # based on size or number of lines.
      class SplittingHandler < Handler

        include Logging

        # Split the `original` file into several files with the same
        # relative path and basename but with a suffix.
        #
        # @param [Pathname] original
        def process_input original
          split(original)
        end

        # Split the `original` file in the input directory into
        # several files in the output directory.
        #
        # @param [Pathname] original
        def split original
          copy = path_for(original)
          mkdir_p(copy.dirname)
          FileUtils.cd(copy.dirname) do
            log.debug("Splitting #{original} -> #{copy}")
            unless settings[:dry_run]
              raise Error.new("Split command exited unsuccessfully") unless system(split_command(original, copy))
                
              # If the input file is empty then no output file is
              # generated after the split. We'd prefer to pass an
              # empty file that looks as though it is part 0 of a
              # split with no other files.  So we touch that 0 file
              # here, just in case it didn't already exist.
              FileUtils.touch(copy.dirname.join(suffix_stem_for(copy) + first_suffix))
            end
            if settings[:metadata] && (! settings[:dry_run])
              Pathname.glob(copy.dirname.join(suffix_stem_for(copy) + "*")).each do |split|
                process_metadata_for(split)
              end
            end
          end
        end

        # Should we split files by counting lines?
        #
        # @return [true, false]
        def split_by_lines?
          settings[:bytes].nil? && settings[:lines]
        end

        # Should we split files by counting bytes?
        #
        # @return [true, false]
        def split_by_bytes?
          settings[:bytes]
        end

        # The path to the `split` program.
        #
        # @return [String]
        def split_program
          settings[:split_program]
        end

        # The file dumper that will be used to dump the file into the
        # `split` command-line.
        #
        # The file dumper is configured with the `clean` option but
        # without the `decorate` option.
        #
        # @param [Pathname] original
        # @return [FileDumper]
        def file_dumper original
          FileDumper.new(input: original, clean: true)
        end

        # Return the command used to dump and split the `original`
        # file into many files at `copy` with a suffix appended.
        #
        # Because of the way the `split` command works, this
        # command-line should be run from parent directory of `copy`.
        #
        # @param [Pathname] original
        # @param [Pathname] copy
        # @return [String]
        def split_command original, copy
          condition = '--' + (split_by_bytes? ? 'bytes' : 'lines') + '=' + (split_by_bytes? ? settings[:bytes] : settings[:lines]).to_s
          "#{file_dumper(original).dump_command} | #{split_program} #{condition} --numeric-suffixes --suffix-length=#{suffix_length} - #{Shellwords.escape(suffix_stem_for(copy))} 2>&1"
        end

        # Return the stem of the suffix used for each produced output
        # file.
        #
        # The actual suffix will have the split number appended.
        #
        # @param [Pathname] copy
        def suffix_stem_for(copy)
          copy.basename.to_s.gsub(%r{/},'') + '.part-'
        end

        # Length of suffix used.
        #
        # @return [Integer]
        def suffix_length
          4
        end

        # Suffix of the first split.
        #
        # @return [String]
        def first_suffix
          "0" * suffix_length
        end

      end
    end
  end
end

