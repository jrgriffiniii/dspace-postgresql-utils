# frozen_string_literal: true

module CLI
  module DSpace
    class MigrationJob
      attr_accessor :query_results
      attr_reader :deleted_item_queue, :missing_items, :duplicated_items

      def initialize(source_repository:, destination_repository:)
        @source_repository = source_repository
        @destination_repository = destination_repository
        @query_results = []

        @migrated_items = []
        @deletions = {}
        @deleted_item_queue = []

        @missing_items = []
        @duplicated_items = []

        @migrations = {}
        @replacements = {}
      end

      def perform
        migrate_from_query_results
      end

      def migrated?(item_id)
        @migrations.key?(item_id)
      end

      def replaced?(item_id)
        @replacements.key?(item_id)
      end

      def deleted?(item_id)
        @deletions.key?(item_id)
      end

      def migrated_source_items
        @migrations.keys
      end

      def migrated_destination_items
        @migrations.values
      end

      def replaced_items
        @replacements.values
      end

      def deleted_items
        @deletions.keys
      end

      private

      def logger
        logger = Logger.new($stdout)
        logger.level = Logger::INFO
        logger
      end

      def create_migration(source:, replaced:, **item_values)
        item_id = source
        replaced_item_id = replaced
        new_item_id = item_values['item_id']

        if migrated?(item_id)
          logger.info "Updating #{item_id}..."
        else
          logger.info "Importing #{item_id}..."
          logger.info "Creating a new Item for #{item_id}..."

          row_values = [new_item_id]
          row_values += item_values.values_at('submitter_id', 'in_archive', 'owning_collection', 'last_modified')

          @destination_repository.connection.insert_item(*row_values)

          @migrations[item_id] = new_item_id
          @replacements[replaced_item_id] = new_item_id
          logger.info "Created #{new_item_id}..."

          @deleted_item_queue << replaced_item_id
        end

        new_item_id
      end

      def find_migration_for(source_item_id)
        @migrations[source_item_id]
      end

      def find_replacement_for(replaced_item_id)
        @replacements[replaced_item_id]
      end

      def migrate_from_query_results
        rows = query_results.to_a

        rows.each do |row|
          item_id = row['item_id']

          next if @missing_items.include?(item_id)
          next if @duplicated_items.include?(item_id)

          # Find the old Item by metadata
          item_titles = @source_repository.connection.find_titles_by_item_id(item_id)

          if replaced?(item_id)
            replaced_item_id = @replacements[item_id]
          else
            replaced_item_ids = []
            item_titles.each do |item_title|
              results = @destination_repository.connection.find_by_title_metadata(item_title)
              replaced_item_ids = results unless results.empty?
            end

            # Skip this import if the matching Item cannot be found
            if replaced_item_ids.empty?
              logger.warn "Failed to find the matching Item for #{item_id} using the titles: #{item_titles.join(', ')}. Skipping the import..."
              @missing_items << item_id
              next
            elsif replaced_item_ids.length > 1
              logger.warn "Found multiple matching Items for #{item_id} using the titles: #{item_titles.join(', ')}. Skipping the import..."
              @duplicated_items << item_id
              next
            end

            replaced_item_id = replaced_item_ids.first
            @replacements[item_id] = replaced_item_id
          end

          new_item_id = @destination_repository.connection.select_new_item_id
          item_values = row.to_h
          item_values['item_id'] = new_item_id

          new_item_id = create_migration(source: item_id, replaced: replaced_item_id, **item_values)

          # Inserting Metadata rows
          metadata_value_values = [new_item_id]
          metadata_value_values += row.values_at('metadata_field_id', 'text_value', 'text_lang', 'resource_type_id')
          @destination_repository.connection.insert_metadata_value(*metadata_value_values)

          schema_name = row['short_id']
          element = row['element']
          qualifier = row['qualifier']

          new_metadata_field = "#{schema_name}.#{element}"
          new_metadata_field += ".#{qualifier}" unless qualifier.nil?

          new_metadata_value = row['text_value']
          logger.info "Created metadata value for #{new_item_id}: #{new_metadata_field}: #{new_metadata_value}..."

          # Query for the community and collection
          updated_collections = []
          updated_communities = []
          @source_repository.connection.select_community_collections(item_id) do |collection_results|
            collection_results.each do |collection_row|
              community_id = collection_row.values_at('community_id').first

              unless updated_communities.include?(community_id)

                # This will assume that the community IDs between the two installations are identical
                @destination_repository.connection.update_community(new_item_id, replaced_item_id, community_id)
                logger.info "Updated the community membership for #{new_item_id} from #{replaced_item_id}..."

                updated_communities << community_id
              end

              collection_id = collection_row.values_at('collection_id').first
              next if updated_collections.include?(collection_id)

              # This will assume that the collection IDs between the two installations are identical
              @destination_repository.connection.update_collection(new_item_id, replaced_item_id, collection_id)
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
          handle = row['handle']
          @destination_repository.connection.update_handle(new_item_id, replaced_item_id, handle)
          logger.info "Updated the handles for #{new_item_id}..."

          # Migrate the workflow item
          @destination_repository.connection.update_workflow_item(new_item_id, replaced_item_id)
          logger.info "Updated the workflow items for #{new_item_id}..."

          # Migrate the workspace item
          @destination_repository.connection.update_workspace_item(new_item_id, replaced_item_id)
          logger.info "Updated the workspace items for #{new_item_id}..."

          @migrated_items << new_item_id unless @migrated_items.include?(new_item_id)
        end

        until @deleted_item_queue.empty?
          deleted_item_id = @deleted_item_queue.shift

          # Deleting Metadata rows
          @destination_repository.connection.delete_metadata_values(deleted_item_id)
          logger.info "Deleting the old metadata values for #{deleted_item_id}..."

          begin
            @destination_repository.connection.delete_item(deleted_item_id)
            logger.info "Deleting the replaced Item #{deleted_item_id}..."

            new_item_id = find_replacement_for(deleted_item_id)
            @deletions[deleted_item_id] = new_item_id

            # Should there be a collision, the ID for the Item in the destination system should be updated first
            # @destination_repository.connection.update_item_id(new_item_id, deleted_item_id)
          rescue StandardError => e
            logger.error "Failed to delete Item #{deleted_item_id}: #{e}"
          end

        end
      end
    end
  end
end
