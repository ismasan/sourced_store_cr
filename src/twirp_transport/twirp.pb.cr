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
        optional :global_seq, :int64, 5
        optional :seq, :int32, 6
        optional :created_at, Google::Protobuf::Timestamp, 7
        optional :metadata, :bytes, 8
        optional :payload, :bytes, 9
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

    struct ReadCategoryRequest
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :category, :string, 1
        optional :batch_size, :int32, 2
        optional :consumer_group, :string, 3
        optional :consumer_id, :string, 4
        optional :wait_timeout, :int32, 5
        optional :last_seq, :int64, 6
      end
    end

    struct ReadCategoryResponse
      include ::Protobuf::Message

      contract_of "proto3" do
        repeated :events, Event, 1
      end
    end

    struct AckConsumerRequest
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :consumer_group, :string, 1
        optional :consumer_id, :string, 2
        optional :last_seq, :int64, 3
      end
    end

    struct AckConsumerResponse
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :successful, :bool, 1
        optional :error, Error, 2
      end
    end
  end
end
