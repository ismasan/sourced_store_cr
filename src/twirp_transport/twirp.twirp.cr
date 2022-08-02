# Generated from protos/twirp.proto by twirp.cr
require "twirp"

module SourcedStore
  module TwirpTransport
    abstract class SourcedStore::TwirpTransport::EventStore
      include Twirp::Service

      @@service_name = "sourced_store.twirp_transport.EventStore"

      rpc ReadStream, receives: ::SourcedStore::TwirpTransport::ReadStreamRequest, returns: ::SourcedStore::TwirpTransport::ReadStreamResponse
    end
  end
end
