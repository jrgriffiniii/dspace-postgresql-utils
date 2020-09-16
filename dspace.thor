# frozen_string_literal: true

require 'thor'
require 'yaml'
require 'ostruct'
require 'logger'

require 'pry-byebug'

require_relative 'cli/dspace/repository'

require_relative 'cli/dspace/migration_job'
require_relative 'cli/dspace/update_handles_job'

require_relative 'cli/dspace/migration_report'
require_relative 'cli/dspace/metadata_report'

class Dspace < Thor
  namespace :dspace

  desc 'update_handles', 'Update Handles using a CSV'
  option :db_config_file_path, type: :string, aliases: '-c'
  option :report_file_path, type: :string, required: true, aliases: '-f'
  def update_handles
    db_config_file_path = options.fetch(:db_config_file, File.join(File.dirname(__FILE__), 'config', 'databases.yml'))
    db_config = build_db_configuration(db_config_file_path)

    dest_db_host = db_config.destination_database.host
    dest_db_port = db_config.destination_database.port
    dest_db_name = db_config.destination_database.name
    dest_db_user = db_config.destination_database.user
    dest_db_password = db_config.destination_database.password

    dest_dspace = CLI::DSpace::Repository.new(dest_db_host, dest_db_port, dest_db_name, dest_db_user, dest_db_password)

    report_file_path = options.fetch(:report_file_path)

    job = CLI::DSpace::UpdateHandlesJob.new(csv_file_path: report_file_path, destination_repository: dest_dspace)
    job.perform
  end

  desc 'generate_metadata_report', 'Generate a CSV report of Item metadata'
  option :db_config_file_path, type: :string, aliases: '-c'
  option :query_field, type: :string, required: true, aliases: '-f'
  option :query_value, type: :string, required: true, aliases: '-v'
  option :exported_fields, type: :string, required: true, aliases: '-e'
  option :output_file, type: :string, required: false, aliases: '-o'
  def generate_metadata_report
    db_config_file_path = options.fetch(:db_config_file, File.join(File.dirname(__FILE__), 'config', 'databases.yml'))
    db_config = build_db_configuration(db_config_file_path)

    db_host = db_config.source_database.host
    db_port = db_config.source_database.port
    db_name = db_config.source_database.name
    db_user = db_config.source_database.user
    db_password = db_config.source_database.password

    report_config_file_path = options.fetch(:report_config_file, File.join(File.dirname(__FILE__), 'config', 'reports.yml'))
    report_config = build_report_configuration(report_config_file_path)

    metadata_field = options.fetch(:query_field)
    metadata_value = options[:query_value]
    exported_fields_value = options[:exported_fields]
    exported_fields = exported_fields_value.split(',')
    output_file = options[:output_file]

    source_dspace = CLI::DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)

    query_results = source_dspace.connection.select_metadata(metadata_field, metadata_value, limit)
    metadata_report = CLI::DSpace::MetadataReport.new(
      query_results: query_results,
      metadata_fields: exported_fields,
      configuration: report_config,
      output_file: output_file
    )

    metadata_report.write
  end

  desc 'migrate_items_by_metadata', 'Migrate a set of Items between DSpace installations, filtered by a specific metadata field and value'
  option :metadata_field, type: :string, required: true, aliases: '-f'
  option :metadata_value, type: :string, required: true, aliases: '-v'
  option :db_config_file_path, type: :string, aliases: '-c'
  option :limit, type: :numeric, required: false, aliases: '-l'
  def migrate_items_by_metadata
    db_config_file_path = options.fetch(:db_config_file, File.join(File.dirname(__FILE__), 'config', 'databases.yml'))
    db_config = build_db_configuration(db_config_file_path)

    db_host = db_config.source_database.host
    db_port = db_config.source_database.port
    db_name = db_config.source_database.name
    db_user = db_config.source_database.user
    db_password = db_config.source_database.password

    dest_db_host = db_config.destination_database.host
    dest_db_port = db_config.destination_database.port
    dest_db_name = db_config.destination_database.name
    dest_db_user = db_config.destination_database.user
    dest_db_password = db_config.destination_database.password

    report_config_file_path = options.fetch(:report_config_file, File.join(File.dirname(__FILE__), 'config', 'reports.yml'))
    report_config = build_report_configuration(report_config_file_path)

    metadata_field = options.fetch(:metadata_field, 'dc.title')
    metadata_value = options[:metadata_value]
    limit = options[:limit]

    source_dspace = CLI::DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)
    dest_dspace = CLI::DSpace::Repository.new(dest_db_host, dest_db_port, dest_db_name, dest_db_user, dest_db_password)

    migration_job = CLI::DSpace::MigrationJob.new(source_repository: source_dspace, destination_repository: dest_dspace)

    query_results = source_dspace.connection.select_items_by_metadata(metadata_field, metadata_value, limit)
    migration_job.query_results = query_results

    migration_job.perform

    migration_report = CLI::DSpace::MigrationReport.new(
      source_repository: source_dspace.database_uri,
      destination_repository: dest_dspace.database_uri,
      migrated_source_items: migration_job.migrated_source_items,
      migrated_destination_items: migration_job.migrated_destination_items,
      replaced_items: migration_job.replaced_items,
      missing_items: migration_job.missing_items,
      deleted_items: migration_job.deleted_items,
      duplicated_items: migration_job.duplicated_items,
      configuration: report_config
    )

    migration_report.write
  end

  no_commands do
    class Config < OpenStruct
      def self.config_file
        File.open(@file_path, 'rb')
      end

      def self.config_values
        file = config_file
        YAML.safe_load(file)
      end

      def self.build(file_path)
        @file_path = file_path
        new(config_values)
      end
    end

    class DatabaseConfig < Config
      def source_database
        ::OpenStruct.new(to_h[:source_database])
      end

      def destination_database
        ::OpenStruct.new(to_h[:destination_database])
      end
    end

    class ReportConfig < Config; end

    def build_db_configuration(file_path)
      DatabaseConfig.build(file_path)
    end

    def build_report_configuration(file_path)
      ReportConfig.build(file_path)
    end
  end
end
