# # Generated from protos/twirp.proto for sourced_store.twirp_transport
require "protobuf"

require "./timestamp.pb.cr"

module SourcedStore
  module TwirpTransport
    struct Event
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :id, :string, 1
        optional :topic, :string, 2
        optional :stream_id, :string, 3
        optional :originator_id, :string, 4
        optional :global_seq, :int64, 5
        optional :seq, :int32, 6
        optional :created_at, Google::Protobuf::Timestamp, 7
        optional :payload, :bytes, 8
      end
    end

    struct Error
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :code, :string, 1
        optional :message, :string, 2
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
        optional :error, Error, 2
      end
    end
  end
end
