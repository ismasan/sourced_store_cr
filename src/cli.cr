require "http/server"
require "logger"
require "./service.cr"

logger = Logger.new(STDOUT, level: Logger::INFO)

puts "hello"
twirp_handler = Twirp::Server.new(SourcedStore::Service.new(logger: logger))
server = HTTP::Server.new(twirp_handler)

address = server.bind_tcp 8080
puts "Listening on http://#{address}"
server.listen
