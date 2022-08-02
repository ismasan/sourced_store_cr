# frozen_string_literal: true

require 'sourced_client/protos/twirp_twirp'

module SourcedClient
  class TransportError < StandardError
    def initialize(resp)
      super resp.error.inspect
    end
  end

  class Client
    def initialize(endpoint: 'http://127.0.0.1:8080/twirp')
      @client = SourcedClient::TwirpTransport::EventStoreClient.new(endpoint)
    end

    def read_stream(stream_id, upto_seq: nil)
      resp = client.read_stream(stream_id: stream_id)
      raise TransportError.new(resp) if resp.error

      resp.data.events
    end

    def append_to_stream(stream_id, events, expected_seq: nil)
      resp = client.append_to_stream(stream_id: stream_id, expected_seq: expected_seq, events: events)
      raise TransportError.new(resp) if resp.error

      resp.data.successful
    end

    private

    attr_reader :client
  end
end
