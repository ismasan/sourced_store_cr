# # Generated from protos/twirp.proto for sourced_store.twirp_transport
require "protobuf"

module SourcedStore
  module TwirpTransport
    struct Event
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :stream_id, :string, 1
        optional :topic, :string, 2
        optional :payload, :bytes, 3
      end
    end

    struct ReadStreamRequest
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :stream_id, :string, 1
        optional :upto_seq, :int32, 2
      end
    end

    struct ReadStreamResponse
      include ::Protobuf::Message

      contract_of "proto3" do
        repeated :events, Event, 1
      end
    end

    struct AppendToStreamRequest
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :stream_id, :string, 1
        optional :expected_seq, :int32, 2
        repeated :events, Event, 3
      end
    end

    struct AppendToStreamResponse
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :successful, :bool, 1
        optional :error, :string, 2
      end
    end
  end
end
