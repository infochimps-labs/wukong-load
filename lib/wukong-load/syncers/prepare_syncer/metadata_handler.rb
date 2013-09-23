require 'digest/md5'
module Wukong
  module Load
    class PrepareSyncer

      # Allows creating metadata files to accompany output files
      # produced by a handler.
      module MetadataHandler

        # Processes the metadata for the `copy` file.
        #
        # This step should only take place if the original file was
        # handled correctly.
        #
        # @param [Pathname] copy
        def process_metadata_for(copy)
          metadata_path = metadata_path_for(copy)
          mkdir_p(metadata_path.dirname)
          File.open(metadata_path, 'w') do |f|
            f.write(MultiJson.dump(metadata_for(copy)))
          end unless settings[:dry_run]
        end

        # Returns the metadata path for the `copy`.
        #
        # @param [Pathname] copy
        # @return [Pathname]
        def metadata_path_for(copy)
          top_level, rest = relative_path_of(copy, current_output_directory).to_s.split('/', 2)
          current_output_directory.join(top_level + '_meta', rest + ".meta")
        end

        # Returns the metadata for the ``copy` file.
        #
        # @param [Pathname] copy
        # @param [Hash]
        def metadata_for(copy)
          {
            path:      relative_path_of(copy, current_output_directory),
            meta_path: relative_path_of(metadata_path_for(copy), current_output_directory),
            size:      File.size(copy),
            md5:       Digest::MD5.file(copy).hexdigest, # streaming
          }
        end

      end
    end
  end
end
