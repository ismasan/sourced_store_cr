require "http/server"
require "./service.cr"

puts "hello"
twirp_handler = Twirp::Server.new(Service.new)
server = HTTP::Server.new(twirp_handler)

address = server.bind_tcp 8080
puts "Listening on http://#{address}"
server.listen
