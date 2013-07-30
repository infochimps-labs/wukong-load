module Wukong
  module Load
    module FTP
      class FTPFileHandler

        attr_accessor :settings

        include Logging

        def initialize settings
          self.settings = settings
        end

        def process path, counter
          log.debug("Processing file #{counter}:\t#{path}")
          link_metadata = _create_link(path, counter)
          _create_metadata_json_file link_metadata
        end

        # Determines the app-specific type_name for this file
        #
        # This `type_name` is used in the following key name structure:
        #   file:
        #     bucket/app_name/type_name/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext
        #   metadata file:
        #     bucket/app_name/type_name_meta/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext.meta
        #
        # == Parameters:
        # path::
        #   A String representing the filename being processed
        #
        # == Returns:
        # A string representing the app-specific type
        #
        def get_type_name(path)
          _convert_pathname(path)
        end

        # Determines the app-specific slug for this file
        #
        # This `slug` is used in the following key name structure:
        #   file:
        #     bucket/app_name/type_name/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext
        #   metadata file:
        #     bucket/app_name/type_name_meta/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext.meta
        #
        # == Parameters:
        # path::
        #   A String representing the filename being processed
        #
        # counter::
        #   A numerically increasing counter for this path
        #
        # == Returns:
        # A string representing the app-specific slug
        #
        def get_slug(path, counter)
          converted_pathname = _convert_pathname(path)
          "#{counter}-#{converted_pathname}-#{File.basename(path)}"
        end

        # Chooses which pieces of file metadata we wish to upload to s3 in .meta files
        #
        # This will be uploaded to the following key on s3:
        #   metadata file:
        #     bucket/app_name/type_name_meta/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext.meta
        #
        # == Parameters:
        # metadata::
        #   A Hash containing metadata that may be useful for downstream processing
        #   Includes the following keys:
        #     :app_name
        #     :type_name
        #     :slug
        #     :key_name
        #     :link_name
        #     :meta_key_name
        #     :meta_link_name
        #     :md5
        #     :filesize
        #
        # == Returns:
        # A string representing the app-specific slug
        #
        def get_filtered_metadata(metadata)
          keys_to_select = [:key_name, :md5, :filesize, :meta_key_name]
          metadata.select{|k,v| keys_to_select.include? k}
        end


        def get_formatted_date_components
          dt = {
            :year  => format('%04d', Time.now.getutc.year),
            :month => format('%02d', Time.now.getutc.month),
            :day   => format('%02d', Time.now.getutc.day),
            :hour  => format('%02d', Time.now.getutc.hour),
            :min   => format('%02d', Time.now.getutc.min),
            :sec   => format('%02d', Time.now.getutc.sec),
          }
          dt.merge({
                     :yyyymmdd => "#{dt[:year]}#{dt[:month]}#{dt[:day]}",
                     :hhmmss   => "#{dt[:hour]}#{dt[:min]}#{dt[:sec]}"
                   })
        end
        
        protected
        
        def _convert_pathname(filename)
          converted_pathname = File.dirname(filename).split(File::SEPARATOR).join("-")
          if converted_pathname.nil? or converted_pathname.length == 0 or converted_pathname == "."
            converted_pathname = "root"
          end
          converted_pathname
        end

        def _construct_link_metadata(filename, counter)
          link_dir = settings[:links]
          dt = get_formatted_date_components
          app_name     = settings[:name].to_s
          type_name    = get_type_name(filename)
          slug         = get_slug(filename, counter)
          key_basename = "#{type_name}-#{dt[:yyyymmdd]}-#{dt[:hhmmss]}-#{slug}"

          # file:
          #   bucket/app_name/type_name/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext
          key_name = File.join(app_name, type_name, dt[:year], dt[:month], dt[:day], key_basename)
          link_name = File.join(link_dir, key_name)

          #   metadata file:
          #     bucket/app_name/type_name_meta/yyyy/mm/dd/type_name-yyyymmdd-hhmmss-slug.ext.meta
          meta_key_name = File.join(app_name, "#{type_name}_meta", dt[:year], dt[:month], dt[:day], "#{key_basename}.meta")
          meta_link_name = File.join(link_dir, meta_key_name)

          # prepare all intermediate directories
          FileUtils.mkdir_p(File.dirname(link_name))
          FileUtils.mkdir_p(File.dirname(meta_link_name))

          file_abs_path = _file_abs_path(filename)
          settings.dup.merge({
                               :app_name => app_name,
                               :type_name => type_name,
                               :slug => slug,
                               :key_name => key_name,
                               :link_name => link_name,
                               :meta_key_name => meta_key_name,
                               :meta_link_name => meta_link_name,
                               :md5 => Digest::MD5.file(file_abs_path).hexdigest,
                               :filesize => File.stat(file_abs_path).size
                             })
        end

        def _file_abs_path(filename)
          file_abs_path = File.join(settings[:output].to_s, settings[:name].to_s, filename)
          file_abs_path
        end

        def _create_link(filename, counter)
          old_name = _file_abs_path(filename)
          if File.exists?(old_name)
            link_metadata = _construct_link_metadata(filename, counter)
            log.debug "Constructing (hard) link #{link_metadata[:link_name]} -> #{old_name}"
            File.link(old_name, link_metadata[:link_name])
          else
            abort("File #{old_name} does not exist, aborting")
          end
          link_metadata
        end

        def _create_metadata_json_file(link_metadata)
          metadata_filename = link_metadata[:meta_link_name]
          File.open(metadata_filename, "w") do |f|
            log.debug "Constructing metadata json file #{metadata_filename}"
            f.write(MultiJson.dump(get_filtered_metadata(link_metadata)))
          end
        end
        
      end
    end
  end
end

