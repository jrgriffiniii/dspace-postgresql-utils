require 'thor'
require 'yaml'
require 'ostruct'
require 'logger'

require 'pry-byebug'

require_relative 'cli/dspace/repository'
require_relative 'cli/dspace/migration_job'

class Dspace < Thor
  option :metadata_field, type: :string, required: true, aliases: '-f'
  option :metadata_value, type: :string, required: true, aliases: '-v'
  option :config_file_path, type: :string, aliases: '-c'

  desc "migrate_items_by_metadata", "Migrate a set of Items between DSpace grouped by a specific metadata field and value"

  def migrate_items_by_metadata
    config_file_path = options.fetch(:config_file, File.join(File.dirname(__FILE__), 'config', 'databases.yml'))
    config = build_configuration(config_file_path)

    db_host = config.source_database.host
    db_port = config.source_database.port
    db_name = config.source_database.name
    db_user = config.source_database.user
    db_password = config.source_database.password

    dest_db_host = config.destination_database.host
    dest_db_port = config.destination_database.port
    dest_db_name = config.destination_database.name
    dest_db_user = config.destination_database.user
    dest_db_password = config.destination_database.password

    metadata_field = options.fetch(:metadata_field, 'dc.title')
    metadata_value = options[:metadata_value]

    source_dspace = CLI::DSpace::Repository.new(db_host, db_port, db_name, db_user, db_password)
    dest_dspace = CLI::DSpace::Repository.new(dest_db_host, dest_db_port, dest_db_name, dest_db_user, dest_db_password)

    migration_job = CLI::DSpace::MigrationJob.new(source_repository: source_dspace, destination_repository: dest_dspace)

    query_results = source_dspace.connection.select_items_by_metadata(metadata_field, metadata_value)
    migration_job.query_results = query_results

    migration_job.perform
  end

  no_commands do
    class Config < OpenStruct

      def source_database
        ::OpenStruct.new(self.to_h[:source_database])
      end

      def destination_database
        ::OpenStruct.new(self.to_h[:destination_database])
      end
    end

    def config_file(file_path)
      File.open(file_path, "rb")
    end

    def config_values(file_path)
      file = config_file(file_path)
      YAML.load(file)
    end

    def build_configuration(file_path)
      values = config_values(file_path)
      Config.new(values)
    end
  end
end

Dspace.start(ARGV)
