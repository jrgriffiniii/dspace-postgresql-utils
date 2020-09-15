# frozen_string_literal: true

require 'csv'

module CLI
  module DSpace
    class MetadataReport
      def initialize(**options)
        @query_results = options[:query_results]
        @metadata_fields = options[:metadata_fields]
        @configuration = options[:configuration]
      end

      def self.build_timestamp
        Time.now.utc.strftime('%Y%m%d%H%M%S')
      end

      def self.headers
        %w[
          item_id
          schema
          element
          qualifier
          value
          language
        ]
      end

      def metadata_schemata
        @metadata_fields.map { |f| f.split('.').first }
      end

      def metadata_elements
        @metadata_fields.map do |f|
          segments = f.split('.')
          if segments.length > 2
            segments[1]
          else
            segments.last
          end
        end
      end

      def metadata_qualifiers
        values = @metadata_fields.map do |f|
          segments = f.split('.')
          segments.last unless segments.length <= 2
        end
        values.reject(&:nil?)
      end

      def nested_row_matches?(row)
        metadata_schemata.include?(row['short_id']) && metadata_elements.include?(row['element']) && (metadata_qualifiers.include?(row['qualifier']) || metadata_qualifiers.length < metadata_elements.length || metadata_qualifiers.empty?)
      end

      def row_matches?(row)
        return nested_row_matches?(row) if @matched_rows.include?(row['item_id'])

        output = metadata_schemata.first == row['short_id'] && metadata_elements.first == row['element'] && (metadata_qualifiers.first == row['qualifier'] || metadata_qualifiers.length < metadata_elements.length || metadata_qualifiers.empty?)

        @matched_rows << row['item_id'] if output

        output
      end

      def selected_rows
        @matched_rows = []
        @query_results.to_a.select { |row| row_matches?(row) }
      end

      def self.denylist
        %w[
          ORIGINAL
          TEXT
          LICENSE
        ]
      end

      def reject?(text_value)
        self.class.denylist.include?(text_value) || /\.pdf$/.match(text_value) || /\.txt$/.match(text_value)
      end

      def rows
        values = []

        selected_rows.each do |row|
          text_value = row['text_value']
          values << row.values_at('item_id', 'short_id', 'element', 'qualifier', 'text_value', 'text_lang') unless reject?(text_value)
        end

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
        "#{self.class.build_timestamp}_metadata_report.csv"
      end

      def report_config
        @configuration.metadata_reports
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
