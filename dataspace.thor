require 'thor'
require 'yaml'
require 'ostruct'
require 'logger'

require 'pry-byebug'

require_relative 'cli/dspace/repository'

class Dataspace < Thor
  option :config_file_path, type: :string, aliases: '-c'
  option :class_year, type: :string, aliases: '-y'

  desc "student_theses_migrate", "Migrate the student theses DataSpace Items"

  def student_theses_migrate
    config_file_path = options.fetch(:config_file, File.join(File.dirname(__FILE__), 'config', 'databases.yml'))
    config = build_configuration(config_file_path)

    db_host = config.source_database.host
    db_port = config.source_database.port
    db_name = config.source_database.name
    db_user = config.source_database.user
    db_password = config.source_database.password

    dest_db_host = config.destination_database.host
    dest_db_port = config.destination_database.port
    dest_db_name = config.destination_database.name
    dest_db_user = config.destination_database.user
    dest_db_password = config.destination_database.password

    class_year = options.fetch(:class_year, '2020')

    prev_dspace = CLI::DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)
    next_dspace = CLI::DSpace::Repository.new(dest_db_host, dest_db_port, dest_db_name, dest_db_user, dest_db_password)
    persisted_items = {}

    prev_dspace.connection.select_items(class_year) do |result|
      result.each do |row|

        new_item_id = next_dspace.connection.select_new_item_id
        item_values = [new_item_id]
        item_values += row.values_at('submitter_id', 'in_archive', 'owning_collection', 'last_modified')
        item_id = row.values_at('item_id').first

        logger.info "Importing #{item_id}..."

        if persisted_items.key?(item_id)
          new_item_id = persisted_items[item_id]
        else
          logger.info "Creating a new Item for #{item_id}..."
          # new_item_id = next_dspace.connection.insert_item(*item_values)
          next_dspace.connection.insert_item(*item_values)

          persisted_items[item_id] = new_item_id
          logger.info "Created #{new_item_id}..."
        end

        # Inserting Metadata rows
        new_metadata_value_id = next_dspace.connection.select_new_metadata_value_id
        metadata_value_values = [ new_metadata_value_id, new_item_id ]
        metadata_value_values += row.values_at('metadata_field_id', 'text_value', 'text_lang', 'resource_type_id')
        next_dspace.connection.insert_metadata_value(*metadata_value_values)
        logger.info "Created metadata value #{new_metadata_value_id}..."

        # Deleting Metadata rows
        next_dspace.connection.delete_metadata_values(item_id)

        # Query for the community and collection
        persisted_communities = {}
        prev_dspace.connection.select_community_collections(item_id) do |collection_results|
          collection_results.each do |collection_row|
            community_id = collection_row.values_at('community_id').first

            if !persisted_communities.key?(community_id)
              next_dspace.connection.update_community(new_item_id, item_id)
              logger.info "Updated the community membership for #{new_item_id}"

              persisted_communities[community_id] = community_id
            end

            next_dspace.connection.update_collection(new_item_id, item_id)
            logger.info "Updated the collection membership for #{new_item_id}"
          end
        end

        # Update the Item policies
        next_dspace.connection.update_resource_policies(new_item_id, item_id)
        logger.info "Updated the Item authorization policies for #{new_item_id}..."

        persisted_bundles = {}
        prev_dspace.connection.select_bundle_bitstreams(item_id) do |bitstream_results|

          bitstream_results.each do |bitstream_row|

            bundle_id = bitstream_row.values_at('bundle_id').first
            primary_bitstream_id = bitstream_row.values_at('primary_bitstream_id').first

            if persisted_bundles.key?(bundle_id)
              new_bundle_id = persisted_bundles[bundle_id]
            else
              # new_bundle_id = next_dspace.connection.insert_bundle(next_id, item_id, primary_bitstream_id)

              new_bundle_id = next_dspace.connection.select_new_bundle_id
              bundle_values = [ new_bundle_id ]
              bundle_values += bitstream_row.values_at('primary_bitstream_id')
              next_dspace.connection.insert_bundle(new_item_id, *bundle_values)
              logger.info "Created the new bundle #{new_bundle_id}..."

              next_dspace.connection.update_resource_policies(bundle_id, new_bundle_id)

              persisted_bundles[bundle_id] = new_bundle_id
            end

            bitstream_id = bitstream_row['bitstream_id']
            bitstream_order = bitstream_row.values_at('bitstream_order').first
            new_bitstream_id = next_dspace.connection.select_new_bitstream_id

            bitstream_values = [new_bitstream_id]
            bitstream_values += bitstream_row.values_at('bitstream_format_id', 'checksum', 'checksum_algorithm', 'internal_id', 'deleted', 'store_number', 'sequence_id', 'size_bytes')

            next_dspace.connection.insert_bitstream(new_bundle_id, new_bitstream_id, bitstream_order, *bitstream_values)
            logger.info "Created the new bitstream #{new_bitstream_id}..."
            next_dspace.connection.update_resource_policies(bitstream_id, new_bitstream_id)
            logger.info "Updating the authorization policies for the new bitstream #{new_bitstream_id}..."
          end
        end

        # Migrate the handles
        next_dspace.connection.update_handle(new_item_id, item_id)
        logger.info "Updated the handles for #{new_item_id}..."

        # Migrate the workflow item
        next_dspace.connection.update_workflow_item(new_item_id, item_id)
        logger.info "Updated the workflow items for #{new_item_id}..."

        # Migrate the workspace item
        next_dspace.connection.update_workspace_item(new_item_id, item_id)
        logger.info "Updated the workspace items for #{new_item_id}..."
      end
    end
  end

  no_commands do
    class Config < OpenStruct

      def source_database
        ::OpenStruct.new(self.to_h[:source_database])
      end

      def destination_database
        ::OpenStruct.new(self.to_h[:destination_database])
      end
    end

    def config_file(file_path)
      File.open(file_path, "rb")
    end

    def config_values(file_path)
      file = config_file(file_path)
      YAML.load(file)
    end

    def build_configuration(file_path)
      values = config_values(file_path)
      Config.new(values)
    end

    def logger
      logger = Logger.new(STDOUT)
      logger.level = Logger::INFO
      logger
    end
  end
end

Dataspace.start(ARGV)
