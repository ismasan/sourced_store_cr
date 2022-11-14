require "admiral"
require "../pg_backend"

module CLI
  class Setup < Admiral::Command
    define_flag database : String, required: true, short: d

    def run
      logger = Logger.new(STDOUT)
      backend = SourcedStore::PGBackend.new(
        logger: logger,
        db_url: flags.database
      )

      backend.setup
      backend.stop
    end
  end
end
