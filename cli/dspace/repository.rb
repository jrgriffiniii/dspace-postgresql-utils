# frozen_string_literal: true

require_relative 'connection'

module CLI
  module DSpace
    class Repository
      def initialize(db_host, db_port, db_name, db_user, db_password)
        @db_host = db_host
        @db_port = db_port
        @db_name = db_name
        @db_user = db_user
        @db_password = db_password
      end

      def connection
        @connection ||= Connection.new(dbname: @db_name, host: @db_host, port: @db_port, user: @db_user, password: @db_password)
      end
    end
  end
end
