
require_relative 'cli/dspace'

class Dataspace < Thor

  option :db_host, :type => :string
  option :db_port, :type => :string
  option :db_name, :type => :string
  option :db_user, :type => :string
  option :class_year, type: :string
  desc "Retrieve the 2020 student theses Items"

  def migrate_student_theses
    db_host_env = ENV.fetch('PGHOST', 'localhost')
    db_port_env = ENV.fetch('PGPORT', 5432)
    db_name_env = ENV.fetch('PGDATABASE', 'database')
    db_user_env = ENV.fetch('PGUSER', 'user')
    db_password = ENV.fetch('PGPASSWORD', 'secret')

    db_host = option.fetch(:db_host, db_host_env)
    db_port = option.fetch(:db_port, db_port_env)
    db_name = option.fetch(:db_name, db_name_env)
    db_user = option.fetch(:db_user, db_user_env)
    class_year = option.fetch(:class_year, '2020')

    prev_dspace = DSpace.new(db_host, db_port, db_name, db_user, db_password)
    next_dspace = DSpace.new(db_host, db_port, db_name, db_user, db_password)
    migrated_items = []

    prev_dspace.select_items(class_year) do |rows|
      rows.each do |row|

        item_values = row.values_at('item.item_id', 'item.submitter_id', 'item.in_archive', 'item.owning_collection', 'item.last_modified')
        item_id = item_values.first

        if !migrated_items.include?(item_id)
          next_dspace.insert_item(*item_values)
          prev_dspace.delete_item(item_id)

          persisted_item[item_id] = item_values
        end

        # Inserting Metadata rows
        metadata_value_values = row.values_at('metadatavalue.metadata_value_id', 'metadatavalue.resource_id', 'metadatavalue.text_value', 'metadatavalue.metadata_field_id', 'metadatavalue.text_value', 'metadatavalue.resource_type_id')
        next_dspace.insert_metadata_value(*metadata_value_values)

        # Deleting Metadata rows
        prev_dspace.delete_metadata_values(item_id)

        # Query for the community and collection
        persisted_communities = []
        prev_dspace.select_community_collections_query(item_id) do |collection_row|

          if !persisted_communities.include?(community_id)
            community_id = collection_row.values_at('comm2i.community_id')
            next_dspace.update_community(next_item_id, item_id)

            persisted_communities << community_id
          end

          collection_id = collection_row.values_at('coll2i.collection_id')
          next_dspace.update_collection(collection_id, item_id)
        end

        # Update the Item policies
        next_dspace.update_resource_policies(next_item_id, item_id)

        persisted_bundles = []
        prev_dspace.select_bundle_bitstreams(item_id) do |bitstream_row|

          bundle_id = bitstream_row.values_at('coll2i.collection_id')
          primary_bitstream_id = bitstream_row.values_at('bundle.primary_bitstream_id')

          if !persisted_bundles.include?(bundle_id)
            new_bundle_id = next_dspace.insert_bundle(next_id, item_id, primary_bitstream_id)
            next_dspace.update_resource_policies(bundle_id, new_bundle_id)

            persisted_bundles << bundle_id
          end

          bitstream_id = bitstream_row.values_at('bitstream.bitstream_id')

          next_dspace.insert_bitstream(bitstream_id, item_id)
          next_dspace.update_resource_policies(bitstream_id, new_bitstream_id)
        end

        # Migrate the handles
        next_dspace.update_handle(next_item_id, item_id)

        # Migrate the workflow item
        next_dspace.update_workflow_item(next_item_id, item_id)

        # Migrate the workspace item
        next_dspace.update_workspace_item(next_item_id, item_id)

      end
    end

  end

  no_commands do
    
      end


end
