require 'pg'

module CLI
  module DSpace
    class Connection
      def build_select_community_collections_query
        "SELECT coll2i.collection_id, comm2coll.community_id FROM community2collection AS comm2coll INNER JOIN collection2item AS coll2i ON coll2i.collection_id=comm2coll.collection_id WHERE coll2i.item_id=$1"
      end

      def select_community_collections_query(item_id)
        statement = build_select_community_collections_query
        execute_statement(statement, item_id)
      end

      def build_update_community_statement
        "UPDATE communities2item SET item_id=$1 WHERE item_id=$2"
      end

      def update_community(next_item_id, item_id)
        statement = build_update_community_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_update_collection_statement
        "UPDATE collection2item SET item_id=$1 WHERE item_id=$2"
      end

      def update_collection(next_item_id, item_id)
        statement = build_update_collection_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_insert_item_statement
        "INSERT INTO item (item_id, submitter_id, in_archive, withdrawn, owning_collection, last_modified, discoverable) VALUES ($1, $2, $3, FALSE, $4, $5, TRUE)"
      end

      def build_select_new_item_id
        "SELECT item_id FROM item ORDER BY item_id DESC LIMIT 1"
      end

      def select_new_item_id
        select_statement = build_select_new_item_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('item_id').first.to_i + 1
      end

      def insert_item(*item_values)
        statement = build_insert_item_statement
        execute_statement(statement, *item_values)
        item_id = select_new_item_id
      end

      def build_delete_item_statement
        "DELETE FROM item WHERE item.item_id=$1"
      end

      def delete_item(item_id)
        statement = build_delete_item_statement
        execute_statement(statement, item_id)
      end

      def build_select_new_metadata_value_id
        "SELECT metadata_value_id FROM metadatavalue ORDER BY metadata_value_id DESC LIMIT 1"
      end

      def select_new_metadata_value_id
        select_statement = build_select_new_metadata_value_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('metadata_value_id').first.to_i + 1
      end

      def build_insert_metadata_value_statement
        "INSERT INTO metadatavalue (metadata_value_id, resource_id, metadata_field_id, text_value, text_lang, resource_type_id) VALUES ($1, $2, $3, $4, $5, $6)"
      end

      def insert_metadata_value(*metadata_values)
        statement = build_insert_metadata_value_statement
        execute_statement(statement, *metadata_values)
      end

      def build_delete_metadata_value_statement
        "DELETE FROM metadatavalue WHERE metadatavalue.resource_id=$1"
      end

      def delete_metadata_values(resource_id)
        statement = build_delete_metadata_value_statement
        execute_statement(statement, resource_id)
      end

      def self.pg_class
        ::PG
      end

      def build_connection
        self.class.pg_class.connect(dbname: @dbname, host: @host, port: @port, user: @user, password: @password)
      end

      def connection
        @connection ||= build_connection
      end

      def initialize(dbname:, host:, port:, user:, password:)
        @dbname = dbname
        @host = host
        @port = port
        @user = user
        @password = password
        @connection = build_connection
      end

      def build_select_items_query
        "SELECT item.item_id, item.submitter_id, item.in_archive, item.withdrawn, item.owning_collection, item.last_modified, item.discoverable, r2.element, r2.qualifier, metadatavalue.metadata_field_id, metadatavalue.text_value, metadatavalue.text_lang, metadatavalue.resource_type_id FROM item INNER JOIN metadatavalue ON metadatavalue.resource_id=item.item_id INNER JOIN metadatafieldregistry AS r2 ON metadatavalue.metadata_field_id=r2.metadata_field_id WHERE (metadatavalue.resource_type_id=2 AND r2.element='title') and item.item_id in (select i.item_id from item as i inner join metadatavalue as v on v.resource_id=i.item_id inner join metadatafieldregistry as r on v.metadata_field_id=r.metadata_field_id where r.element = 'date' and r.qualifier='classyear' and v.text_value=$1 and v.resource_type_id=2)"
      end

      def select_items(class_year)
        query = build_select_items_query
        yield execute_statement(query, class_year)
      end

      def build_update_resource_policies_statement
        "UPDATE resourcepolicy SET resource_id=$1 WHERE resource_id=$2"
      end

      def update_resource_policies(next_item_id, item_id)
        statement = build_update_resource_policies_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_select_bundle_bitstreams_query
        "SELECT bitstream.*, bundle.*, b2b.bitstream_id, b2b.bundle_id FROM bundle2bitstream AS b2b INNER JOIN item2bundle AS i2b ON i2b.bundle_id=b2b.bundle_id INNER JOIN bundle ON bundle.bundle_id=b2b.bundle_id INNER JOIN bitstream ON bitstream.bitstream_id=b2b.bitstream_id WHERE i2b.item_id=$1"
      end

      def select_bundle_bitstreams(item_id)
        statement = build_select_bundle_bitstreams_query
        execute_statement(statement, item_id)
      end

      def build_insert_bundle_statement
        "INSERT into bundle (bitstream_id, primary_bitstream_id) VALUES ($1, $2)"
      end

      def build_update_bundle_statement
        "INSERT INTO item2bundle (id, item_id, bundle_id) VALUES ($1, $2, $3)"
      end

      def build_select_new_bundle_id
        "SELECT bundle_id FROM bundle ORDER BY bundle_id DESC LIMIT 1"
      end

      def select_new_bundle_id
        select_statement = build_select_new_bundle_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('bundle_id').first.to_i + 1
      end

      def build_next_item_to_bundle_id
        "SELECT id FROM item2bundle ORDER BY id DESC LIMIT 1"
      end

      def select_next_item_to_bundle_id
        select_statement = build_next_item_to_bundle_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('id').first.to_i + 1
      end

      def insert_bundle(next_item_id, *bundle_values)
        insert_statement = build_insert_bundle_statement
        # bundle_id = execute_statement(insert_statement, *bundle_values)
        bundle_id = bundle_values.first
        execute_statement(insert_statement, *bundle_values)

        next_item_to_bundle_id = select_next_item_to_bundle_id
        update_statement = build_update_bundle_statement
        execute_statement(update_statement, next_item_to_bundle_id, next_item_id, bundle_id)
        new_bundle_id = select_new_bundle_id
      end

      def build_insert_bitstream_statement
        "INSERT into bitstream (bitstream_id, bitstream_format_id, checksum, checksum_algorithm, internal_id, deleted, store_number, sequence_id, size_bytes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
      end

      def build_update_bitstream_statement
        "INSERT INTO bundle2bitstream (id, bundle_id, bitstream_id, bitstream_order) VALUES ($1, $2, $3, $4)"
      end

      def build_select_new_bitstream_id
        "SELECT bitstream_id FROM bitstream ORDER BY bitstream_id DESC LIMIT 1"
      end

      def select_new_bitstream_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('bitstream_id').first.to_i + 1
      end

      def insert_bitstream(bundle_id, bitstream_order, *bitstream_values)
        insert_statement = build_insert_bitstream_statement
        bitstream_id = execute_statement(insert_statement, *bitstream_values)

        update_statement = build_update_bitstream_statement
        execute_statement(update_statement, bundle_id, bitstream_id, bitstream_order)

        bitstream_id = select_new_bitstream_id
      end

      def build_update_handle_statement
        "UPDATE handle SET resource_id=$1 WHERE resource_id=$2"
      end

      def update_handle(next_item_id, item_id)
        statement = build_update_handle_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_update_workflow_item_statement
        "UPDATE workflowitem SET item_id=$1 WHERE item_id=$2"
      end

      def update_workflow_item(next_item_id, item_id)
        statement = build_update_workflow_item_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_update_workspace_item_statement
        "UPDATE workspaceitem SET item_id=$1 WHERE item_id=$2"
      end

      def update_workspace_item(next_item_id, item_id)
        statement = build_update_workspace_item_statement
        execute_statement(statement, next_item_id, item_id)
      end

      private

      def execute_statement(statement, *params)
        connection.exec_params(statement, params)
      end
    end
  end
end
