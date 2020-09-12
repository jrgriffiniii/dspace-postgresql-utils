require_relative 'cli/dspace/repository'
require 'thor'

class Dataspace < Thor
  # Source database
  option :db_host, type: :string
  option :db_port, type: :string
  option :db_name, type: :string
  option :db_user, type: :string
  option :class_year, type: :string
  desc "student_theses_migrate", "Migrate the student theses DataSpace Items"

  def student_theses_migrate
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

    prev_dspace = DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)
    next_dspace = DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)
    persisted_items = {}

    prev_dspace.select_items(class_year) do |rows|
      rows.each do |row|

        item_values = row.values_at('item.item_id', 'item.submitter_id', 'item.in_archive', 'item.owning_collection', 'item.last_modified')
        item_id = item_values.first

        if persisted_items.key?(item_id)
          new_item_id = persisted_items[item_id]
        else
          new_item_id = next_dspace.insert_item(*item_values)

          persisted_item[item_id] = new_item_id
        end

        # Inserting Metadata rows
        metadata_value_values = row.values_at('metadatavalue.metadata_value_id')
        metadata_value_values << new_item_id
        metadata_value_values += row.values_at('metadatavalue.text_value', 'metadatavalue.metadata_field_id', 'metadatavalue.text_value', 'metadatavalue.resource_type_id')
        next_dspace.insert_metadata_value(*metadata_value_values)

        # Deleting Metadata rows
        next_dspace.delete_metadata_values(item_id)

        # Query for the community and collection
        persisted_communities = {}
        prev_dspace.select_community_collections_query(item_id) do |collection_row|

          if !persisted_communities.key?(community_id)
            community_id = collection_row.values_at('comm2i.community_id').first
            next_dspace.update_community(next_item_id, item_id)
            persisted_communities[community_id] = community_id
          end

          next_dspace.update_collection(next_item_id, item_id)
        end

        # Update the Item policies
        next_dspace.update_resource_policies(next_item_id, item_id)

        persisted_bundles = {}
        prev_dspace.select_bundle_bitstreams(item_id) do |bitstream_row|

          bundle_id = bitstream_row.values_at('b2b.bundle_id').first
          primary_bitstream_id = bitstream_row.values_at('bundle.primary_bitstream_id').first

          if persisted_bundles.key?(bundle_id)
            new_bundle_id = persisted_bundles[bundle_id]
          else
            new_bundle_id = next_dspace.insert_bundle(next_id, item_id, primary_bitstream_id)
            next_dspace.update_resource_policies(bundle_id, new_bundle_id)

            persisted_bundles[bundle_id] = new_bundle_id
          end

          bitstream_order = row.values_at('b2b.bitstream_order').first
          bitstream_values = row.values_at('bitstream.bitstream_format_id', 'bitstream.checksum', 'bitstream.checksum_algorithm', 'bitstream.internal_id', 'bitstream.deleted', 'bitstream.store_number', 'bitstream.sequence_id', 'bitstream.size_bytes')
          new_bitstream_id = next_dspace.insert_bitstream(new_bundle_id, bitstream_order, *bitstream_values)
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
end

Dataspace.start(ARGV)
