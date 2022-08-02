require "twirp"
require "twirp/server"

require "./twirp_transport/twirp.twirp.cr"
require "./twirp_transport/twirp.pb.cr"

module SourcedStore
  class Service < SourcedStore::TwirpTransport::EventStore
    def initialize(logger : Logger)
      @logger = logger
    end

    def read_stream(req : TwirpTransport::ReadStreamRequest) : TwirpTransport::ReadStreamResponse
      payload = [TwirpTransport::Event::PayloadEntry.new(key: "name", value: "Ismael")]

      event = TwirpTransport::Event.new(
        stream_id: req.stream_id,
        topic: "carts.items.added",
        payload: payload
      )

      TwirpTransport::ReadStreamResponse.new(
        events: [event]
      )
    end

    def append_to_stream(req : TwirpTransport::AppendToStreamRequest) : TwirpTransport::AppendToStreamResponse
      @logger.info "Appending events to stream '#{req.stream_id}'"
      @logger.info req.events.inspect
      TwirpTransport::AppendToStreamResponse.new(
        successful: true
      )
    end
  end
end
