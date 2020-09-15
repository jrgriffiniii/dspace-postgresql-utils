# frozen_string_literal: true

require 'csv'

module CLI
  module DSpace
    class UpdateHandlesJob
      def initialize(**options)
        @csv_file_path = options[:csv_file_path]
        @destination_repository = options[:destination_repository]
        @logger = options[:logger]
      end

      def csv_headers
        # csv_entries.first
        csv.headers
      end

      def csv_rows
        # csv_entries[1..-1]
        csv.each.to_a
      end

      def csv_handle_column_index
        csv_headers.index('dc.identifier.uri')
      end

      def csv_title_column_index
        csv_headers.index('dc.title')
      end

      def handles
        csv_rows.map { |row| row[csv_handle_column_index] }
      end

      def self.handle_uri_base
        'http://arks.princeton.edu/ark:/'
      end

      def self.handle_metadata_field
        'dc.identifier.uri'
      end

      def update_metadata(identifier_uri, resource_id)
        results = @destination_repository.connection.update_metadata_by_resource_id(self.class.handle_metadata_field, identifier_uri, resource_id)
        rows = results.to_a

        if rows.empty?
          metadata_field_results = @destination_repository.connection.find_metadata_field_id(self.class.handle_metadata_field)
          metadata_field_id = metadata_field_results.first

          metadata_values = [
            resource_id,
            metadata_field_id,
            identifier_uri,
            nil,
            2
          ]
          insert_results = @destination_repository.connection.insert_metadata_value(*metadata_values)
          rows = insert_results.to_a
          if rows.empty?
            logger.warn "Failed to update #{resource_id} with the identifier #{identifier_uri}"
          else
            row = rows.first
            metadata_value_id = row['metadata_value_id']
            logger.info "Inserted the metadata record #{metadata_value_id} for #{resource_id} with #{identifier_uri}"
          end
        else
          row = rows.first
          metadata_value_id = row['metadata_value_id']
          logger.info "Updated the metadata record #{metadata_value_id} for #{resource_id} with #{identifier_uri}"
        end

        metadata_value_id
      end

      def update_handle(handle, title)
        begin
          results = @destination_repository.connection.update_handle_by_title(handle, title)
          rows = results.to_a
          if rows.empty?
            logger.warn "Failed to update #{handle} for #{title}"
            id_results = @destination_repository.connection.find_by_title_metadata(title)
            resource_id = id_results.first
          else
            row = rows.first
            resource_id = row['resource_id']
            logger.info "Updated #{handle} for #{resource_id}"
          end
        rescue PG::UniqueViolation
          logger.debug "Handle #{handle} already set for the resources matching #{title}"
          id_results = @destination_repository.connection.find_by_title_metadata(title)

          resource_id = id_results.first
          update_results = @destination_repository.connection.update_handle_by_handle(handle, resource_id)
          rows = update_results.to_a
          if rows.empty?
            logger.warn "Failed to update #{handle} for #{resource_id}"
          else
            logger.info "Updated #{handle} for #{resource_id}"
          end
        end

        resource_id
      end

      def perform
        csv_rows.each do |row|
          identifier_uri = row[csv_handle_column_index]
          handle = identifier_uri.gsub(self.class.handle_uri_base, '')
          title = row[csv_title_column_index]

          resource_id = update_handle(handle, title)

          if resource_id.nil?
            logger.warn "Failed to resolve the resource ID for #{handle} and #{title}"
          else
            update_metadata(identifier_uri, resource_id)
          end
        end
      end

      private

      def logger
        @logger ||= begin
                      new_logger = Logger.new($stdout)
                      new_logger.level = Logger::INFO
                      new_logger
                    end
      end

      def csv_file
        File.open(@csv_file_path, 'rb')
      end

      def csv
        @csv ||= CSV.parse(csv_file, headers: true)
      end
    end
  end
end
