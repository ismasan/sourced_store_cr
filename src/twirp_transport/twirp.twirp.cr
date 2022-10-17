# Generated from protos/twirp.proto by twirp.cr
require "twirp"

require "./timestamp.pb.cr"

module SourcedStore
  module TwirpTransport
    abstract class SourcedStore::TwirpTransport::EventStore
      include Twirp::Service

      @@service_name = "sourced_store.twirp_transport.EventStore"

      rpc ReadStream, receives: ::SourcedStore::TwirpTransport::ReadStreamRequest, returns: ::SourcedStore::TwirpTransport::ReadStreamResponse
      rpc AppendToStream, receives: ::SourcedStore::TwirpTransport::AppendToStreamRequest, returns: ::SourcedStore::TwirpTransport::AppendToStreamResponse
      rpc ReadCategory, receives: ::SourcedStore::TwirpTransport::ReadCategoryRequest, returns: ::SourcedStore::TwirpTransport::ReadCategoryResponse
      rpc AckConsumer, receives: ::SourcedStore::TwirpTransport::AckConsumerRequest, returns: ::SourcedStore::TwirpTransport::AckConsumerResponse
    end
  end
end
