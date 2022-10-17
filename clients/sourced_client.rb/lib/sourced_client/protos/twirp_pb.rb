# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: protos/twirp.proto

require 'google/protobuf'

require 'google/protobuf/timestamp_pb'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("protos/twirp.proto", :syntax => :proto3) do
    add_message "sourced_store.twirp_transport.Event" do
      optional :id, :string, 1
      optional :topic, :string, 2
      optional :stream_id, :string, 3
      optional :originator_id, :string, 4
      optional :global_seq, :int64, 5
      optional :seq, :int32, 6
      optional :created_at, :message, 7, "google.protobuf.Timestamp"
      optional :payload, :bytes, 8
    end
    add_message "sourced_store.twirp_transport.Error" do
      optional :code, :string, 1
      optional :message, :string, 2
    end
    add_message "sourced_store.twirp_transport.ReadStreamRequest" do
      optional :stream_id, :string, 1
      optional :upto_seq, :int32, 2
    end
    add_message "sourced_store.twirp_transport.ReadStreamResponse" do
      repeated :events, :message, 1, "sourced_store.twirp_transport.Event"
    end
    add_message "sourced_store.twirp_transport.AppendToStreamRequest" do
      optional :stream_id, :string, 1
      optional :expected_seq, :int32, 2
      repeated :events, :message, 3, "sourced_store.twirp_transport.Event"
    end
    add_message "sourced_store.twirp_transport.AppendToStreamResponse" do
      optional :successful, :bool, 1
      optional :error, :message, 2, "sourced_store.twirp_transport.Error"
    end
    add_message "sourced_store.twirp_transport.ReadCategoryRequest" do
      optional :category, :string, 1
      optional :after_global_seq, :int64, 2
      optional :batch_size, :int32, 3
      optional :consumer_group, :string, 4
      optional :consumer_id, :string, 5
      optional :wait_timeout, :int32, 6
    end
    add_message "sourced_store.twirp_transport.ReadCategoryResponse" do
      repeated :events, :message, 1, "sourced_store.twirp_transport.Event"
    end
    add_message "sourced_store.twirp_transport.AckConsumerRequest" do
      optional :consumer_group, :string, 1
      optional :consumer_id, :string, 2
      optional :last_seq, :int64, 3
    end
    add_message "sourced_store.twirp_transport.AckConsumerResponse" do
      optional :successful, :bool, 1
      optional :error, :message, 2, "sourced_store.twirp_transport.Error"
    end
  end
end

module SourcedClient
  module TwirpTransport
    Event = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.Event").msgclass
    Error = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.Error").msgclass
    ReadStreamRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.ReadStreamRequest").msgclass
    ReadStreamResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.ReadStreamResponse").msgclass
    AppendToStreamRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.AppendToStreamRequest").msgclass
    AppendToStreamResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.AppendToStreamResponse").msgclass
    ReadCategoryRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.ReadCategoryRequest").msgclass
    ReadCategoryResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.ReadCategoryResponse").msgclass
    AckConsumerRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.AckConsumerRequest").msgclass
    AckConsumerResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("sourced_store.twirp_transport.AckConsumerResponse").msgclass
  end
end
