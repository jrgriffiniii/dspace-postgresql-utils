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
        'UPDATE communities2item SET item_id=$1 WHERE item_id=$2'
      end

      def update_community(next_item_id, item_id)
        statement = build_update_community_statement
        execute_statement(statement, next_item_id, item_id)
      end

      def build_update_collection_statement
        'UPDATE collection2item SET item_id=$1 WHERE item_id=$2'
      end

      def update_collection(next_item_id, item_id)
        statement = build_update_collection_statement
        execute_statement(statement, next_item_id, item_id)
      end

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

      def build_delete_item_statement
        'DELETE FROM item WHERE item.item_id=$1'
      end

      def delete_item(item_id)
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
        'INSERT INTO metadatavalue (metadata_value_id, resource_id, metadata_field_id, text_value, text_lang, resource_type_id) VALUES ($1, $2, $3, $4, $5, $6)'
      end

      def insert_metadata_value(*metadata_values)
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
        SELECT i2.item_id, i2.submitter_id, i2.in_archive, i2.withdrawn, i2.owning_collection, i2.last_modified, i2.discoverable, schema2.short_id, r2.element, r2.qualifier, v2.metadata_field_id,v2.text_value, v2.text_lang, v2.resource_type_id
          FROM item AS i2
          INNER JOIN metadatavalue AS v2 ON v2.resource_id=i2.item_id
          INNER JOIN metadatafieldregistry AS r2 ON v2.metadata_field_id=r2.metadata_field_id
          INNER JOIN metadataschemaregistry AS schema2 ON schema2.metadata_schema_id=r2.metadata_schema_id

          WHERE i2.item_id IN (

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
        SELECT i2.item_id, i2.submitter_id, i2.in_archive, i2.withdrawn, i2.owning_collection, i2.last_modified, i2.discoverable, schema2.short_id, r2.element, r2.qualifier, v2.metadata_field_id,v2.text_value, v2.text_lang, v2.resource_type_id
          FROM item AS i2
          INNER JOIN metadatavalue AS v2 ON v2.resource_id=i2.item_id
          INNER JOIN metadatafieldregistry AS r2 ON v2.metadata_field_id=r2.metadata_field_id
          INNER JOIN metadataschemaregistry AS schema2 ON schema2.metadata_schema_id=r2.metadata_schema_id

          WHERE i2.item_id IN (

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
              LIMIT $5
          )
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

      def build_select_item_by_metadata_query
        <<-SQL
        SELECT i2.item_id, i2.submitter_id, i2.in_archive, i2.withdrawn, i2.owning_collection, i2.last_modified, i2.discoverable, s2.short_id, r2.element, r2.qualifier, v2.metadata_field_id,v2.text_value, v2.text_lang, v2.resource_type_id
          FROM item AS i2
          INNER JOIN metadatavalue AS v2 ON v2.resource_id=i2.item_id
          INNER JOIN metadatafieldregistry AS r2 ON r2.metadata_field_id=v2.metadata_field_id
          INNER JOIN metadataschemaregistry AS s2 ON s2.metadata_schema_id=r2.metadata_schema_id

          WHERE i2.item_id IN (

            SELECT i.item_id FROM item AS i
              INNER JOIN metadatavalue AS v ON v.resource_id=i.item_id
              INNER JOIN metadatafieldregistry AS r ON r.metadata_field_id=v.metadata_field_id
              INNER JOIN metadataschemaregistry AS s ON s.metadata_schema_id=r.metadata_schema_id
              WHERE s.short_id=$1
                AND r.element=$2
                AND r.qualifier=$3
                AND v.text_value=$4
              GROUP BY item_id
              LIMIT 1
          )
        SQL
      end

      def select_item_by_metadata(metadata_field, metadata_value)
        query = build_select_item_by_metadata_query
        schema_name, metadata_field_element, metadata_field_qualifier = metadata_field.split('.')
        execute_statement(query, schema_name, metadata_field_element, metadata_field_qualifier, metadata_value)
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
        'INSERT into bundle (bundle_id, primary_bitstream_id) VALUES ($1, $2)'
      end

      def build_insert_item_to_bundle_statement
        'INSERT INTO item2bundle (id, item_id, bundle_id) VALUES ($1, $2, $3)'
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

      def insert_bundle(next_item_id, *bundle_values)
        bundle_id = bundle_values.first
        insert_statement = build_insert_bundle_statement
        execute_statement(insert_statement, *bundle_values)

        next_item_to_bundle_id = select_next_item_to_bundle_id
        insert_i2b_statement = build_insert_item_to_bundle_statement
        execute_statement(insert_i2b_statement, next_item_to_bundle_id, next_item_id, bundle_id)
      end

      def build_insert_bitstream_statement
        'INSERT into bitstream (bitstream_id, bitstream_format_id, checksum, checksum_algorithm, internal_id, deleted, store_number, sequence_id, size_bytes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)'
      end

      def build_update_bitstream_statement
        'INSERT INTO bundle2bitstream (id, bundle_id, bitstream_id, bitstream_order) VALUES ($1, $2, $3, $4)'
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
        'UPDATE handle SET resource_id=$1 WHERE resource_id=$2'
      end

      def update_handle(next_item_id, item_id)
        statement = build_update_handle_statement
        execute_statement(statement, next_item_id, item_id)
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
