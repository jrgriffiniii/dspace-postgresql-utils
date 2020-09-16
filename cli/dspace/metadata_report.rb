# frozen_string_literal: true

require 'csv'

module CLI
  module DSpace
    class MetadataReport
      def initialize(**options)
        @query_results = options[:query_results]
        @metadata_fields = options[:metadata_fields]
        @configuration = options[:configuration]
        @output_file_path = options[:output_file]
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

      def headers
        ['item_id', *@metadata_fields]
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

      def row_matches?(row, field_index = 0)
        return if @metadata_fields.length < field_index + 1

        return row_matches?(row, field_index + 1) if @matched_rows.include?(row['item_id']) && @metadata_fields.length > field_index + 1

        output = metadata_schemata[field_index] == row['short_id'] && metadata_elements[field_index] == row['element'] && (metadata_qualifiers[field_index] == row['qualifier'] || metadata_qualifiers.length < metadata_elements.length || metadata_qualifiers.empty?)

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
        indexed_values = {}

        selected_rows.each do |row|
          text_value = row['text_value']
          next if reject?(text_value)

          item_id = row['item_id']
          if indexed_values.key?(item_id)
            # indexed_values[item_id] += row.values_at('short_id', 'element', 'qualifier', 'text_value', 'text_lang')
            indexed_values[item_id] += row.values_at('text_value')
          else
            # indexed_values[item_id] = row.values_at('item_id', 'short_id', 'element', 'qualifier', 'text_value', 'text_lang')
            indexed_values[item_id] = row.values_at('item_id', 'text_value')
          end
        end

        indexed_values.values
      end

      def generate
        output = CSV.generate do |csv|
          # csv << self.class.headers + @metadata_fields
          csv << headers

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
        @output_file_path ||= begin
                                value = File.join(output_dir_path, output_file_name)
                                Pathname.new(value)
                              end
      end

      def output_file
        File.open(output_file_path, 'wb')
      end
    end
  end
end
