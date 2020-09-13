# frozen_string_literal: true

module CLI
  module DSpace
    class MigrationJob
      attr_accessor :query_results
      attr_reader :migrated_items, :deleted_items

      def initialize(source_repository:, destination_repository:)
        @source_repository = source_repository
        @destination_repository = destination_repository
        @query_results = []

        @migrated_items = []
        @deleted_items = []
      end

      def perform
        migrate_from_query_results
      end

      private

      def logger
        logger = Logger.new($stdout)
        logger.level = Logger::INFO
        logger
      end

      def migrate_from_query_results
        persisted_items = {}
        replaced_items = {}
        unmatched_items = []
        duplicated_items = []
        item_deletion_queue = []

        rows = query_results.to_a

        rows.each do |row|
          new_item_id = @destination_repository.connection.select_new_item_id
          item_values = [new_item_id]
          item_values += row.values_at('submitter_id', 'in_archive', 'owning_collection', 'last_modified')
          item_id = row['item_id']
          next if unmatched_items.include?(item_id)
          next if duplicated_items.include?(item_id)

          # Find the old Item by metadata
          item_titles = @source_repository.connection.find_titles_by_item_id(item_id)

          if replaced_items.key?(item_id)
            replaced_item_id = replaced_items[item_id]
          else
            replaced_item_ids = []
            item_titles.each do |item_title|
              results = @destination_repository.connection.find_by_title_metadata(item_title)
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
            @destination_repository.connection.insert_item(*item_values)

            persisted_items[item_id] = new_item_id
            logger.info "Created #{new_item_id}..."

            item_deletion_queue << replaced_item_id
          end

          # Inserting Metadata rows
          new_metadata_value_id = @destination_repository.connection.select_new_metadata_value_id
          metadata_value_values = [new_metadata_value_id, new_item_id]
          # One should ensure that the field in the desination repository actually *exists* before attempting to create the new metadata record
          metadata_value_values += row.values_at('metadata_field_id', 'text_value', 'text_lang', 'resource_type_id')

          schema_name = row['short_id']
          element = row['element']
          qualifier = row['qualifier']

          new_metadata_field = "#{schema_name}.#{element}"
          new_metadata_field += ".#{qualifier}" unless qualifier.nil?
          new_metadata_value = row['text_value']

          begin
            @destination_repository.connection.insert_metadata_value(*metadata_value_values)
          rescue StandardError => e
            @logger.error "The metadata field for #{new_metadata_field} failed. Does this field exist in the registry for the destination repository?"
            @logger.error e
            next
          end

          logger.info "Created metadata value #{new_metadata_value_id}: #{new_metadata_field}: #{new_metadata_value}..."

          # Query for the community and collection
          updated_collections = []
          updated_communities = []
          @destination_repository.connection.select_community_collections(replaced_item_id) do |collection_results|
            collection_results.each do |collection_row|
              community_id = collection_row.values_at('community_id').first

              unless updated_communities.include?(community_id)
                @destination_repository.connection.update_community(new_item_id, replaced_item_id)
                logger.info "Updated the community membership for #{new_item_id} from #{replaced_item_id}..."

                updated_communities << community_id
              end

              collection_id = collection_row.values_at('collection_id').first
              next if updated_collections.include?(collection_id)

              @destination_repository.connection.update_collection(new_item_id, replaced_item_id)
              logger.info "Updated the collection membership for #{new_item_id} from #{replaced_item_id}..."

              updated_collections << collection_id
            end
          end

          @destination_repository.connection.update_resource_policies(new_item_id, replaced_item_id)
          logger.info "Updated the Item authorization policies for #{new_item_id}..."

          persisted_bundles = {}
          @destination_repository.connection.select_bundle_bitstreams(replaced_item_id) do |bitstream_results|
            bitstream_results.each do |bitstream_row|
              bundle_id = bitstream_row.values_at('bundle_id').first

              next if persisted_bundles.key?(bundle_id)

              @destination_repository.connection.update_bundle(new_item_id, replaced_item_id)
              logger.info "Updated the bundle for #{new_item_id} from #{replaced_item_id}..."

              persisted_bundles[bundle_id] = bundle_id
            end
          end

          # Migrate the handles
          @destination_repository.connection.update_handle(new_item_id, replaced_item_id)
          logger.info "Updated the handles for #{new_item_id}..."

          # Migrate the workflow item
          @destination_repository.connection.update_workflow_item(new_item_id, replaced_item_id)
          logger.info "Updated the workflow items for #{new_item_id}..."

          # Migrate the workspace item
          @destination_repository.connection.update_workspace_item(new_item_id, replaced_item_id)
          logger.info "Updated the workspace items for #{new_item_id}..."

          @migrated_items << new_item_id unless @migrated_items.include?(new_item_id)
        end

        until item_deletion_queue.empty?
          deleted_item_id = item_deletion_queue.shift

          next if @deleted_items.include?(deleted_item_id)

          # Deleting Metadata rows
          @destination_repository.connection.delete_metadata_values(deleted_item_id)
          logger.info "Deleting the old metadata values for #{deleted_item_id}..."

          begin
            @destination_repository.connection.delete_item(deleted_item_id)
            logger.info "Deleting the replaced Item #{deleted_item_id}..."
          rescue StandardError => e
            logger.warn "Failed to delete Item #{deleted_item_id}: #{e}"
          end

          @deleted_items << deleted_item_id
        end
      end
    end
  end
end
