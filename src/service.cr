require "twirp"
require "twirp/server"

require "./twirp_transport/twirp.twirp.cr"
require "./twirp_transport/twirp.pb.cr"

class Service < TwirpTransport::EventStore
  def read_stream(req : TwirpTransport::ReadStreamRequest) : TwirpTransport::ReadStreamResponse
    event = TwirpTransport::Event.new(
      stream_id: req.stream_id,
      topic: "carts.items.added"
    )

    TwirpTransport::ReadStreamResponse.new(
      events: [event]
    )
  end
end
