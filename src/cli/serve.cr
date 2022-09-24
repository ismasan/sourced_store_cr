require "http/server"
require "logger"
require "admiral"
require "../service.cr"

module CLI
  class Serve < Admiral::Command
    define_flag port : Int32, default: 8080, short: p
    define_flag database : String, required: true, short: d

    # DB_URL = "postgres://localhost/carts_development"

    def run
      logger = Logger.new(STDOUT, level: Logger::INFO)
      service = SourcedStore::Service.new(logger: logger, db_url: flags.database)

      Signal::INT.trap do
        service.stop
        puts "bye (int)"
        exit
      end

      Signal::TERM.trap do
        service.stop
        puts "bye (term)"
        exit
      end

      twirp_handler = Twirp::Server.new(service)
      server = HTTP::Server.new(twirp_handler)
      address = server.bind_tcp flags.port
      puts "Listening on http://#{address}"
      server.listen
    end
  end
end
