# Generated from protos/twirp.proto by twirp.cr
require "twirp"

require "./struct.pb.cr"

module TwirpTransport
  abstract class TwirpTransport::EventStore
    include Twirp::Service

    @@service_name = "twirp_transport.EventStore"

    rpc ReadStream, receives: ::TwirpTransport::ReadStreamRequest, returns: ::TwirpTransport::ReadStreamResponse
  end
end
