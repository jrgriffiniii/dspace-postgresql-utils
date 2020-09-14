# frozen_string_literal: true

require 'csv'

module CLI
  module DSpace
    class MigrationReport
      def initialize(**options)
        @source_repository = options[:source_repository]
        @destination_repository = options[:destination_repository]
        @migrated_source_items = options[:migrated_source_items]
        @migrated_dest_items = options[:migrated_destination_items]
        @replaced_items = options[:replaced_items]
        @missing_items = options[:missing_items]
        @deleted_items = options[:deleted_items]
        @duplicated_items = options[:duplicated_items]
        @configuration = options[:configuration]
      end

      def self.build_timestamp
        Time.now.utc.strftime('%Y%m%d%H%M%S')
      end

      def self.headers
        %w[
          item_id
          database
          state
        ]
      end

      def rows
        values = []

        migrated_rows = @migrated_source_items.map { |i| [i, @source_repository, 'MIGRATED'] }
        migrated_rows += @migrated_dest_items.map { |i| [i, @destination_repository, 'MIGRATED'] }
        replaced_rows = @replaced_items.map { |i| [i, @destination_repository, 'REPLACED'] }
        missing_rows = @missing_items.map { |i| [i, @destination_repository, 'MISSING'] }
        duplicated_rows = @duplicated_items.map { |i| [i, @destination_repository, 'DELETED'] }

        values += migrated_rows
        values += replaced_rows
        values += missing_rows
        values += duplicated_rows

        values
      end

      def generate
        output = CSV.generate do |csv|
          csv << self.class.headers

          rows.each do |row|
            csv << row
          end
        end

        @csv = output
      end

      def write
        generate if @csv.nil?
        output_file.write(@csv)
      end

      private

      def output_file_name
        "#{self.class.build_timestamp}_migration_report.csv"
      end

      def report_config
        @configuration.migration_reports
      end

      def output_dir_path
        relative = Pathname.new(report_config['directory'])
        Pathname.new(relative.realpath)
      end

      def output_file_path
        value = File.join(output_dir_path, output_file_name)
        Pathname.new(value)
      end

      def output_file
        File.open(output_file_path, 'wb')
      end
    end
  end
end
