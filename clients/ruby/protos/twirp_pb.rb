# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: protos/twirp.proto

require 'google/protobuf'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("protos/twirp.proto", :syntax => :proto3) do
    add_message "twirp_transport.ReadStreamRequest" do
      optional :stream_id, :string, 1
    end
    add_message "twirp_transport.ReadStreamResponse" do
      repeated :events, :message, 1, "twirp_transport.Event"
    end
    add_message "twirp_transport.Event" do
      optional :stream_id, :string, 1
      optional :topic, :string, 2
    end
  end
end

module TwirpTransport
  ReadStreamRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("twirp_transport.ReadStreamRequest").msgclass
  ReadStreamResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("twirp_transport.ReadStreamResponse").msgclass
  Event = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("twirp_transport.Event").msgclass
end
