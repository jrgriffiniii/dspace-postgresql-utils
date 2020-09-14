# frozen_string_literal: true

require 'pg'

module CLI
  module DSpace
    class Connection
      def build_select_community_collections_query
        'SELECT coll2i.collection_id, comm2coll.community_id FROM community2collection AS comm2coll INNER JOIN collection2item AS coll2i ON coll2i.collection_id=comm2coll.collection_id WHERE coll2i.item_id=$1'
      end

      def select_community_collections(item_id)
        statement = build_select_community_collections_query
        yield execute_statement(statement, item_id)
      end

      def build_update_community_statement
        'UPDATE communities2item SET item_id=$1 WHERE item_id=$2 AND community_id=$3'
      end

      def update_community(next_item_id, item_id, community_id)
        statement = build_update_community_statement
        execute_statement(statement, next_item_id, item_id, community_id)
      end

      def build_update_collection_statement
        'UPDATE collection2item SET item_id=$1 WHERE item_id=$2 AND collection_id=$3'
      end

      def update_collection(next_item_id, item_id, collection_id)
        statement = build_update_collection_statement
        execute_statement(statement, next_item_id, item_id, collection_id)
      end

      # Here one should try to preserve the Item ID from the source system
      def build_insert_item_statement
        'INSERT INTO item (item_id, submitter_id, in_archive, withdrawn, owning_collection, last_modified, discoverable) VALUES ($1, $2, $3, FALSE, $4, $5, TRUE)'
      end

      def build_select_new_item_id
        'SELECT item_id FROM item ORDER BY item_id DESC LIMIT 1'
      end

      def select_new_item_id
        select_statement = build_select_new_item_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('item_id').first.to_i + 1
      end

      def build_delete_item_to_bundle_statement
        'DELETE FROM item2bundle WHERE item2bundle.item_id=$1'
      end

      def delete_item_to_bundle_by_item(item_id)
        statement = build_delete_item_to_bundle_statement
        execute_statement(statement, item_id)
      end

      def build_delete_communities_to_item_statement
        'DELETE FROM communities2item AS c2i WHERE c2i.item_id=$1'
      end

      def delete_communities_to_item(item_id)
        statement = build_delete_communities_to_item_statement
        execute_statement(statement, item_id)
      end

      def build_delete_collection_to_item_statement
        'DELETE FROM collection2item AS c2i WHERE c2i.item_id=$1'
      end

      def delete_collection_to_item(item_id)
        statement = build_delete_collection_to_item_statement
        execute_statement(statement, item_id)
      end

      def build_delete_item_statement
        'DELETE FROM item WHERE item.item_id=$1'
      end

      def delete_item(item_id)
        delete_communities_to_item(item_id)
        delete_collection_to_item(item_id)
        delete_item_to_bundle_by_item(item_id)

        statement = build_delete_item_statement
        execute_statement(statement, item_id)
      end

      def build_select_item_by_title_query
        "SELECT i.item_id FROM item as i INNER JOIN metadatavalue AS v ON v.resource_id=i.item_id INNER JOIN metadatafieldregistry AS r ON r.metadata_field_id=v.metadata_field_id WHERE r.metadata_schema_id=1 AND r.element='title' AND r.qualifier IS NULL AND v.text_value=$1"
      end

      def find_by_title_metadata(title)
        select_statement = build_select_item_by_title_query
        rows = execute_statement(select_statement, title)
        rows.to_a.map { |row| row['item_id'] }
      end

      def build_select_title_by_item_query
        "SELECT v.text_value FROM item as i INNER JOIN metadatavalue AS v ON v.resource_id=i.item_id INNER JOIN metadatafieldregistry AS r ON r.metadata_field_id=v.metadata_field_id WHERE r.metadata_schema_id=1 AND r.element='title' AND r.qualifier IS NULL AND i.item_id=$1"
      end

      def find_titles_by_item_id(item_id)
        select_statement = build_select_title_by_item_query
        rows = execute_statement(select_statement, item_id)
        rows.to_a.map { |row| row['text_value'] }
      end

      def insert_item(*item_values)
        statement = build_insert_item_statement
        execute_statement(statement, *item_values)
      end

      def build_update_item_id_statement
        'UPDATE item SET item_id=$1 WHERE item_id=$2'
      end

      def build_update_collection_to_item_statement
        'UPDATE collection2item SET item_id=$1 WHERE item_id=$2'
      end

      def update_collection_to_item(new_item_id, prev_item_id)
        statement = build_update_collection_to_item_statement
        execute_statement(statement, new_item_id, prev_item_id)
      end

      # This should be removed before the PR is merged
      def update_item_id(item_id)
        new_item_id = select_new_item_id

        update_collection_to_item(new_item_id, item_id)

        statement = build_update_item_id_statement
        execute_statement(statement, new_item_id, item_id)
      end

      def build_select_new_metadata_value_id
        'SELECT metadata_value_id FROM metadatavalue ORDER BY metadata_value_id DESC LIMIT 1'
      end

      def select_new_metadata_value_id
        select_statement = build_select_new_metadata_value_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('metadata_value_id').first.to_i + 1
      end

      def build_insert_metadata_value_statement
        'INSERT INTO metadatavalue (metadata_value_id, resource_id, metadata_field_id, text_value, text_lang, resource_type_id) VALUES (DEFAULT, $1, $2, $3, $4, $5)'
      end

      def alter_sequence_metadatavalue
        new_id = select_new_metadata_value_id
        statement = "ALTER SEQUENCE metadatavalue_seq RESTART WITH #{new_id}"
        execute_statement(statement)
      end

      def insert_metadata_value(*metadata_values)
        # This is a hack, I am not certain why this is needed
        alter_sequence_metadatavalue
        statement = build_insert_metadata_value_statement
        execute_statement(statement, *metadata_values)
      end

      def build_delete_metadata_value_statement
        'DELETE FROM metadatavalue WHERE metadatavalue.resource_id=$1'
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

      def build_select_items_by_metadata_query
        <<-SQL
        SELECT i2.item_id, i2.submitter_id, i2.in_archive, i2.withdrawn, i2.owning_collection, i2.last_modified, i2.discoverable, schema2.short_id, r2.element, r2.qualifier, v2.metadata_field_id,v2.text_value, v2.text_lang, v2.resource_type_id, h.handle
          FROM item AS i2
          INNER JOIN metadatavalue AS v2 ON v2.resource_id=i2.item_id
          INNER JOIN metadatafieldregistry AS r2 ON v2.metadata_field_id=r2.metadata_field_id
          INNER JOIN metadataschemaregistry AS schema2 ON schema2.metadata_schema_id=r2.metadata_schema_id
          INNER JOIN handle AS h ON h.resource_id=i2.item_id

          WHERE v2.resource_type_id=2
            AND i2.item_id IN (

              SELECT i.item_id
                FROM item AS i
                INNER JOIN metadatavalue AS v ON v.resource_id=i.item_id
                INNER JOIN metadatafieldregistry AS r ON v.metadata_field_id=r.metadata_field_id
                INNER JOIN metadataschemaregistry AS schema ON schema.metadata_schema_id=r.metadata_schema_id
                WHERE v.text_value=$4
                  AND v.resource_type_id=2
                  AND schema.short_id=$1
                  AND r.element=$2
                  AND r.qualifier=$3
          )
        SQL
      end

      def build_select_limited_items_by_metadata_query
        <<-SQL
        SELECT i3.item_id, i3.submitter_id, i3.in_archive, i3.withdrawn, i3.owning_collection, i3.last_modified, i3.discoverable, schema3.short_id, r3.element, r3.qualifier, v3.metadata_field_id, v3.text_value, v3.text_lang, v3.resource_type_id
          FROM item AS i3
          INNER JOIN metadatavalue AS v3 ON v3.resource_id=i3.item_id
          INNER JOIN metadatafieldregistry AS r3 ON v3.metadata_field_id=r3.metadata_field_id
          INNER JOIN metadataschemaregistry AS schema3 ON schema3.metadata_schema_id=r3.metadata_schema_id

          WHERE i3.item_id in (
            SELECT distinct(i2.item_id)
              FROM item AS i2
              INNER JOIN metadatavalue AS v2 ON v2.resource_id=i2.item_id

              WHERE v2.resource_type_id=2
                AND i2.item_id IN (

                  SELECT i.item_id
                    FROM item AS i
                    INNER JOIN metadatavalue AS v ON v.resource_id=i.item_id
                    INNER JOIN metadatafieldregistry AS r ON v.metadata_field_id=r.metadata_field_id
                    INNER JOIN metadataschemaregistry AS schema ON schema.metadata_schema_id=r.metadata_schema_id

                    WHERE v.text_value=$4
                      AND v.resource_type_id=2
                      AND schema.short_id=$1
                      AND r.element=$2
                      AND r.qualifier=$3

                    GROUP BY item_id
                    LIMIT $5
              )
          );
        SQL
      end

      def select_items_by_metadata(metadata_field, metadata_value, limit = nil)
        schema_name, metadata_field_element, metadata_field_qualifier = metadata_field.split('.')
        if limit.nil?
          query = build_select_items_by_metadata_query
          execute_statement(query, schema_name, metadata_field_element, metadata_field_qualifier, metadata_value)
        else
          query = build_select_limited_items_by_metadata_query
          execute_statement(query, schema_name, metadata_field_element, metadata_field_qualifier, metadata_value, limit)
        end
      end

      def select_item_by_metadata(metadata_field, metadata_value)
        query = build_select_items_by_metadata_query
        schema_name, metadata_field_element, metadata_field_qualifier = metadata_field.split('.')
        execute_statement(query, schema_name, metadata_field_element, metadata_field_qualifier, metadata_value, 1)
      end

      def build_update_resource_policies_statement
        'UPDATE resourcepolicy SET resource_id=$1 WHERE resource_id=$2'
      end

      def update_resource_policies(next_item_id, item_id)
        statement = build_update_resource_policies_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_select_bundle_bitstreams_query
        'SELECT bitstream.*, bundle.*, b2b.bitstream_id, b2b.bundle_id, b2b.bitstream_order FROM bundle2bitstream AS b2b INNER JOIN item2bundle AS i2b ON i2b.bundle_id=b2b.bundle_id INNER JOIN bundle ON bundle.bundle_id=b2b.bundle_id INNER JOIN bitstream ON bitstream.bitstream_id=b2b.bitstream_id WHERE i2b.item_id=$1'
      end

      def select_bundle_bitstreams(item_id)
        statement = build_select_bundle_bitstreams_query
        yield execute_statement(statement, item_id)
      end

      def build_insert_bundle_statement
        'INSERT into bundle (bundle_id, primary_bitstream_id) VALUES (DEFAULT, $1)'
      end

      def build_insert_item_to_bundle_statement
        'INSERT INTO item2bundle (id, item_id, bundle_id) VALUES (DEFAULT, $1, $2)'
      end

      def build_update_item_to_bundle_statement
        'UPDATE item2bundle SET item_id=$1 WHERE item_id=$2'
      end

      def build_select_new_bundle_id
        'SELECT bundle_id FROM bundle ORDER BY bundle_id DESC LIMIT 1'
      end

      def select_new_bundle_id
        select_statement = build_select_new_bundle_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('bundle_id').first.to_i + 1
      end

      def build_next_item_to_bundle_id
        'SELECT id FROM item2bundle ORDER BY id DESC LIMIT 1'
      end

      def select_next_item_to_bundle_id
        select_statement = build_next_item_to_bundle_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('id').first.to_i + 1
      end

      def update_bundle(next_item_id, prev_item_id)
        update_statement = build_update_item_to_bundle_statement
        execute_statement(update_statement, next_item_id, prev_item_id)
      end

      # This might be deprecated
      def insert_bundle(next_item_id, *bundle_values)
        bundle_id = bundle_values.first
        insert_statement = build_insert_bundle_statement
        execute_statement(insert_statement, *bundle_values)

        insert_i2b_statement = build_insert_item_to_bundle_statement
        execute_statement(insert_i2b_statement, next_item_id, bundle_id)
      end

      # Deprecated
      def build_insert_bitstream_statement
        'INSERT into bitstream (bitstream_id, bitstream_format_id, checksum, checksum_algorithm, internal_id, deleted, store_number, sequence_id, size_bytes) VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)'
      end

      # Deprecated
      def build_insert_bundle_to_bitstream_statement
        'INSERT INTO bundle2bitstream (id, bundle_id, bitstream_id, bitstream_order) VALUES (DEFAULT, $1, $2, $3)'
      end

      def build_select_new_bitstream_id
        'SELECT bitstream_id FROM bitstream ORDER BY bitstream_id DESC LIMIT 1'
      end

      def select_new_bitstream_id
        select_statement = build_select_new_bitstream_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('bitstream_id').first.to_i + 1
      end

      def build_select_new_bundle_to_bitstream_id
        'SELECT id FROM bundle2bitstream ORDER BY id DESC LIMIT 1'
      end

      def select_new_bundle_to_bitstream_id
        select_statement = build_select_new_bundle_to_bitstream_id
        rows = execute_statement(select_statement)
        row = rows.first
        row.values_at('id').first.to_i + 1
      end

      def insert_bitstream(bundle_id, bitstream_id, bitstream_order, *bitstream_values)
        insert_statement = build_insert_bitstream_statement
        execute_statement(insert_statement, *bitstream_values)

        update_statement = build_update_bitstream_statement
        new_bundle_to_bitstream_id = select_new_bundle_to_bitstream_id
        execute_statement(update_statement, new_bundle_to_bitstream_id, bundle_id, bitstream_id, bitstream_order)
      end

      def build_update_handle_statement
        'UPDATE handle SET resource_id=$1, handle=$3 WHERE resource_id=$2'
      end

      def update_handle(next_item_id, item_id, handle)
        statement = build_update_handle_statement
        execute_statement(statement, next_item_id, item_id, handle)
      end

      def build_update_workflow_item_statement
        'UPDATE workflowitem SET item_id=$1 WHERE item_id=$2'
      end

      def update_workflow_item(next_item_id, item_id)
        statement = build_update_workflow_item_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_update_workspace_item_statement
        'UPDATE workspaceitem SET item_id=$1 WHERE item_id=$2'
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
