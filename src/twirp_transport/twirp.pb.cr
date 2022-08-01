# # Generated from protos/twirp.proto for twirp_transport
require "protobuf"

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

    contract_of "proto3" do
      optional :stream_id, :string, 1
      optional :topic, :string, 2
    end
  end
end
