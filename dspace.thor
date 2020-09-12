require 'thor'
require 'yaml'
require 'ostruct'
require 'logger'

require 'pry-byebug'

require_relative 'cli/dspace/repository'

class Dspace < Thor
  option :metadata_field, type: :string, required: true, aliases: '-f'
  option :metadata_value, type: :string, required: true, aliases: '-v'
  option :config_file_path, type: :string, aliases: '-c'

  desc "migrate_items_by_metadata", "Migrate a set of Items between DSpace grouped by a specific metadata field and value"

  def migrate_items_by_metadata
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

    metadata_field = options.fetch(:metadata_field, 'dc.title')
    metadata_value = options[:metadata_value]

    prev_dspace = CLI::DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)
    next_dspace = CLI::DSpace::Repository.new(dest_db_host, dest_db_port, dest_db_name, dest_db_user, dest_db_password)

    persisted_items = {}
    replaced_items = {}
    unmatched_items = []
    duplicated_items = []
    item_deletion_queue = []
    deleted_items = []

    prev_dspace.connection.select_items_by_metadata(metadata_field, metadata_value) do |result|

      item_rows = result.to_a
      item_rows.each do |row|

        new_item_id = next_dspace.connection.select_new_item_id
        item_values = [new_item_id]
        item_values += row.values_at('submitter_id', 'in_archive', 'owning_collection', 'last_modified')
        item_id = row['item_id']
        next if unmatched_items.include?(item_id)
        next if duplicated_items.include?(item_id)

        # Find the old Item by metadata
        item_titles = prev_dspace.connection.find_titles_by_item_id(item_id)

        if replaced_items.key?(item_id)
          replaced_item_id = replaced_items[item_id]
        else
          replaced_item_ids = []
          item_titles.each do |item_title|
            results = next_dspace.connection.find_by_title_metadata(item_title)
            replaced_item_ids = results unless results.empty?
          end

          # Skip this import if the matching Item cannot be found
          if replaced_item_ids.empty?
            logger.warn "Failed to find the matching Item for #{item_id} using the titles: #{item_titles.join(', ')}. Skipping the import..."
            unmatched_items << item_id
            next
          elsif replaced_item_ids.length > 1
            logger.warn "Found multiple matching Items for #{item_id} using the titles: #{item_titles.join(', ')}. Skipping the import..."
            duplicated_items << item_id
            next
          end

          replaced_item_id = replaced_item_ids.first
          replaced_items[item_id] = replaced_item_id
        end

        if persisted_items.key?(item_id)
          logger.info "Updating #{item_id}..."
          new_item_id = persisted_items[item_id]
        else
          logger.info "Importing #{item_id}..."
          logger.info "Creating a new Item for #{item_id}..."
          next_dspace.connection.insert_item(*item_values)

          persisted_items[item_id] = new_item_id
          logger.info "Created #{new_item_id}..."

          item_deletion_queue << replaced_item_id
        end

        # Inserting Metadata rows
        new_metadata_value_id = next_dspace.connection.select_new_metadata_value_id
        metadata_value_values = [ new_metadata_value_id, new_item_id ]
        metadata_value_values += row.values_at('metadata_field_id', 'text_value', 'text_lang', 'resource_type_id')
        next_dspace.connection.insert_metadata_value(*metadata_value_values)

        schema_name = row['short_id']
        element = row['element']
        qualifier = row['qualifier']
        new_metadata_field = "#{schema_name}.#{element}"
        new_metadata_field += ".#{qualifier}" unless qualifier.nil?
        new_metadata_value = row['text_value']
        logger.info "Created metadata value #{new_metadata_value_id}: #{new_metadata_field}: #{new_metadata_value}..."

        # Query for the community and collection
        updated_collections = []
        updated_communities = []
        next_dspace.connection.select_community_collections(replaced_item_id) do |collection_results|
          collection_results.each do |collection_row|
            community_id = collection_row.values_at('community_id').first

            if !updated_communities.include?(community_id)
              next_dspace.connection.update_community(new_item_id, replaced_item_id)
              logger.info "Updated the community membership for #{new_item_id} from #{replaced_item_id}..."

              updated_communities << community_id
            end

            collection_id = collection_row.values_at('collection_id').first
            if !updated_collections.include?(collection_id)
              next_dspace.connection.update_collection(new_item_id, replaced_item_id)
              logger.info "Updated the collection membership for #{new_item_id} from #{replaced_item_id}..."

              updated_collections << collection_id
            end
          end
        end

        next_dspace.connection.update_resource_policies(new_item_id, replaced_item_id)
        logger.info "Updated the Item authorization policies for #{new_item_id}..."

        persisted_bundles = {}
        next_dspace.connection.select_bundle_bitstreams(replaced_item_id) do |bitstream_results|

          bitstream_results.each do |bitstream_row|

            bundle_id = bitstream_row.values_at('bundle_id').first

            if !persisted_bundles.key?(bundle_id)
              next_dspace.connection.update_bundle(new_item_id, replaced_item_id)
              logger.info "Updated the bundle for #{new_item_id} from #{replaced_item_id}..."

              persisted_bundles[bundle_id] = bundle_id
            end
          end
        end

        # Migrate the handles
        next_dspace.connection.update_handle(new_item_id, replaced_item_id)
        logger.info "Updated the handles for #{new_item_id}..."

        # Migrate the workflow item
        next_dspace.connection.update_workflow_item(new_item_id, replaced_item_id)
        logger.info "Updated the workflow items for #{new_item_id}..."

        # Migrate the workspace item
        next_dspace.connection.update_workspace_item(new_item_id, replaced_item_id)
        logger.info "Updated the workspace items for #{new_item_id}..."

        while !item_deletion_queue.empty?
          deleted_item_id = item_deletion_queue.shift

          if !deleted_items.include?(deleted_item_id)
            # Deleting Metadata rows
            next_dspace.connection.delete_metadata_values(deleted_item_id)
            logger.info "Deleting the old metadata values for #{deleted_item_id}..."

            begin
              next_dspace.connection.delete_item(deleted_item_id)
              logger.info "Deleting the replaced Item #{deleted_item_id}..."
            rescue StandardError => error
              logger.warn "Failed to delete Item #{deleted_item_id}: #{error}"
            end

            deleted_items << deleted_item_id
          end
        end
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

Dspace.start(ARGV)
