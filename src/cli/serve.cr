require "http/server"
require "twirp/server"
require "logger"
require "admiral"
require "../service.cr"

module CLI
  class Serve < Admiral::Command
    define_flag port : Int32, default: 8080, short: p, description: "port to run server on"
    define_flag database : String, required: true, short: d, description: "database URL"
    define_flag liveness_timeout : Int32, default: 10, short: l, description: "consumers will be considered 'active' while idle up to this time, in milliseconds"
    define_flag compact_every : Int32, default: 1800, description: "compact consumer groups log every X seconds"
    define_flag snapshot_every : Int32, default: 100, description: "snapshot consumer groups every Z events"
    define_flag keep_snapshots : Int32, default: 1, description: "keep this many snapshots per consumer group stream when compacting"
    define_flag log_level : String, default: "info", description: "log level (info, debug)"

    # DB_URL = "postgres://localhost/carts_development"

    def run
      log_level = case flags.log_level
                  when "info"
                    Logger::INFO
                  when "debug"
                    Logger::DEBUG
                  else
                    Logger::INFO
                  end

      logger = Logger.new(STDOUT, level: log_level)
      backend = SourcedStore::PGBackend.new(
        logger: logger,
        db_url: flags.database,
        liveness_timeout: flags.liveness_timeout.milliseconds,
        compact_every: flags.compact_every.seconds,
        snapshot_every: flags.snapshot_every,
        keep_snapshots: flags.keep_snapshots
      )
      service = SourcedStore::Service.new(backend)

      Signal::INT.trap do
        service.stop
        logger.info "bye (int)"
        exit
      end

      Signal::TERM.trap do
        service.stop
        logger.info "bye (term)"
        exit
      end

      twirp_handler = Twirp::Server.new(service)
      server = HTTP::Server.new(twirp_handler)
      address = server.bind_tcp flags.port
      logger.info "Listening on http://#{address} #{service.info}"
      server.listen
    end
  end
end
