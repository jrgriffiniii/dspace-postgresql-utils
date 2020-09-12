module CLI
  module DSpace
    class Connection
      def build_select_community_collections_query
        "SELECT comm2i.community_id, coll2i.collection_id FROM communities2item AS comm2i INNER JOIN collection2item AS coll2i ON coll2i.community_id=comm2i.community_id WHERE comm2i.item_id=comm2i.community_id WHERE comm2i.item_id=$1"
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
        "INSERT INTO item (item_id, submitter_id, in_archive, withdrawn, owning_collection, last_modified, discoverable) VALUES ($1, $2, $3, FALSE, $4, TRUE)"
      end

      def build_select_new_item_id
        "SELECT item_id FROM item ORDER BY item_id DESC LIMIT 1"
      end

      def select_new_item_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('item_id')
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

      def build_insert_metadata_value_statement
        "INSERT INTO metadatavalue (%d, %d, %d, %s, %d) VALUES ($1, $2, $3, $4, $5)"
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
        pg_class.connect(dbname: @dbname, host: @host, port: @port, user: @user, password: @password)
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
        "SELECT item.item_id, item.submitter_id, item.in_archive, item.withdrawn, item.owning_collection, item.last_modified, item.discoverable, r2.element, r2.qualifier, metadatavalue.text_value, metadatavalue.resource_type_id FROM item INNER JOIN metadatavalue ON metadatavalue.resource_id=item.item_id INNER JOIN metadatafieldregistry AS r2 ON metadatavalue.metadata_field_id=r2.metadata_field_id WHERE (metadatavalue.resource_type_id=2 AND r2.element='title') and item.item_id in (select i.item_id from item as i inner join metadatavalue as v on v.resource_id=i.item_id inner join metadatafieldregistry as r on v.metadata_field_id=r.metadata_field_id where r.element = 'date' and r.qualifier='classyear' and v.text_value=$1 and v.resource_type_id=2)"
      end

      def select_items(class_year)
        query = build_select_items_query
        connection.execute_statement(query, class_year)
      end

      def build_update_resource_policies_statement
        "UPDATE resourcepolicy SET resource_id=$1 WHERE resource_id=$2"
      end

      def update_resource_policies(next_item_id, item_id)
        statement = build_update_resource_policies_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_select_bundle_bitstreams_query
        "SELECT bitstream.*, bundler.*, b2b.bitstream_id, b2b.bundle_id FROM bundle2bitstream AS b2b INNER JOIN item2bundle AS i2b ON i2b.bundle_id=b2b.bundle_id INNER JOIN bundle ON bundle.bundle_id=b2b.bundle_id INNER JOIN bitsteam ON bitstream.bitstream_id=b2b.bitstream_id WHERE i2b.item_id=$1"
      end

      def select_bundle_bitstreams(item_id)
        statement = build_select_bundle_bitstreams_query
        execute_statement(statement, item_id)
      end

      def build_insert_bundle_statement
        "INSERT into bundle (primary_bitstream_id) VALUES ($1)"
      end

      def build_update_bundle_statement
        "INSERT INTO item2bundle (item_id, bundle_id) VALUES ($1, $2)"
      end

      def build_select_new_bundle_id
        "SELECT bundle_id FROM bundle ORDER BY bundle_id DESC LIMIT 1"
      end

      def select_new_bundle_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('bundle_id')
      end

      # I can't tell if this should be update or insert
      def insert_bundle(next_item_id, *bundle_values)
        insert_statement = build_insert_bundle_statement
        bundle_id = execute_statement(insert_statement, *bundle_values)

        update_statement = build_update_bundle_statement
        execute_statement(update_statement, next_item_id, bundle_id)
        new_bundle_id = select_new_bundle_id
      end

      def build_insert_bitstream_statement
        "INSERT into bitstream ($1, $2, $3, $4, $5, $6, $7, $8) VALUES (bitstream_format_id, checksum, checksum_algorithm, internal_id, deleted, store_number, sequence_id, size_bytes)"
      end

      def build_update_bitstream_statement
        "INSERT INTO bundle2bitstream (bundle_id, bitstream_id, bitstream_order) VALUES ($1, $2, $3)"
      end

      def build_select_new_bitstream_id
        "SELECT bitstream_id FROM bitstream ORDER BY bitstream_id DESC LIMIT 1"
      end

      def select_new_bitstream_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('bitstream_id')
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
        connection.exec_params(statement, *params)
      end
    end
  end
end
