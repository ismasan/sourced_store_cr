# Code generated by protoc-gen-twirp_ruby 1.6.0, DO NOT EDIT.
require 'twirp'
require_relative 'twirp_pb.rb'

module SourcedClient
  module TwirpTransport
    class EventStoreService < Twirp::Service
      package 'sourced_store.twirp_transport'
      service 'EventStore'
      rpc :ReadStream, ReadStreamRequest, ReadStreamResponse, :ruby_method => :read_stream
      rpc :AppendToStream, AppendToStreamRequest, AppendToStreamResponse, :ruby_method => :append_to_stream
      rpc :ReadCategory, ReadCategoryRequest, ReadCategoryResponse, :ruby_method => :read_category
    end

    class EventStoreClient < Twirp::Client
      client_for EventStoreService
    end
  end
end
