require "admiral"
require "./cli/serve.cr"
require "./cli/setup.cr"

module CLI
  class Root < Admiral::Command
    register_sub_command serve : Serve, description: "Serve event store"
    register_sub_command setup : Setup, description: "Setup database"

    def run
      puts help_sub_commands
    end
  end
end

CLI::Root.run
