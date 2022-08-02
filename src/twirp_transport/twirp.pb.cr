# # Generated from protos/twirp.proto for sourced_store.twirp_transport
require "protobuf"

module SourcedStore
  module TwirpTransport
    struct ReadStreamRequest
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :stream_id, :string, 1
      end
    end

    struct ReadStreamResponse
      include ::Protobuf::Message

      contract_of "proto3" do
        repeated :events, Event, 1
      end
    end

    struct Event
      include ::Protobuf::Message

      struct PayloadEntry
        include ::Protobuf::Message

        contract_of "proto3" do
          optional :key, :string, 1
          optional :value, :string, 2
        end
      end

      contract_of "proto3" do
        optional :stream_id, :string, 1
        optional :topic, :string, 2
        repeated :payload, Event::PayloadEntry, 3
      end
    end
  end
end
