require "twirp"

require "./backend_interface"
require "./pg_backend"
require "./twirp_transport/twirp.twirp.cr"
require "./twirp_transport/twirp.pb.cr"

module SourcedStore
  alias EventList = Array(TwirpTransport::Event)

  class Service < SourcedStore::TwirpTransport::EventStore
    def initialize(@backend : BackendInterface)

    end

    def info
      @backend.info
    end

    def append_to_stream!(stream_id : String, events : EventList) : Bool
      result = @backend.append_to_stream!(stream_id, events.as(EventList).map { |e| EventRecord.from_proto(e)})
      result.successful
    end

    def append_to_stream(req : TwirpTransport::AppendToStreamRequest) : TwirpTransport::AppendToStreamResponse
      events = req.events.as(EventList).map { |evt| EventRecord.from_proto(evt) }

      result = @backend.append_to_stream(
        stream_id: req.stream_id.as(String),
        events: events
      )

      if result.error
        err = result.error.as(SourcedStore::Error)
        TwirpTransport::AppendToStreamResponse.new(
          successful: false,
          error: TwirpTransport::Error.new(
            code: err.code,
            message: err.message
          )
        )
      else
        TwirpTransport::AppendToStreamResponse.new(successful: true)
      end
    end

    def read_stream(req : TwirpTransport::ReadStreamRequest) : TwirpTransport::ReadStreamResponse
      events = @backend.read_stream(req.stream_id.as(String), req.upto_seq)

      TwirpTransport::ReadStreamResponse.new(events: events.map(&.to_proto))
    end

    def read_category(req : SourcedStore::TwirpTransport::ReadCategoryRequest) : SourcedStore::TwirpTransport::ReadCategoryResponse
      category = req.category.as(String)
      consumer_group : String = req.consumer_group || "global-group"
      consumer_id : String = req.consumer_id || "global-consumer"
      batch_size : Int32 = req.batch_size || 50
      wait_timeout : Time::Span? = req.wait_timeout ? req.wait_timeout.as(Int32).milliseconds : nil

      events = @backend.read_category(category, consumer_group, consumer_id, req.last_seq, batch_size, wait_timeout)
      TwirpTransport::ReadCategoryResponse.new(events: events.map(&.to_proto))
    end

    # Here for tests. Fix this.
    def ack_consumer(consumer_group : String, consumer_id : String, last_seq : Int64?) : Bool
      @backend.ack_consumer(consumer_group, consumer_id, last_seq)
    end

    def ack_consumer(req : TwirpTransport::AckConsumerRequest) : TwirpTransport::AckConsumerResponse
      success = @backend.ack_consumer(
        consumer_group: req.consumer_group.as(String),
        consumer_id: req.consumer_id.as(String),
        last_seq: req.last_seq.as(Int64)
      )
      TwirpTransport::AckConsumerResponse.new(
        successful: success
      )
    end

    def stop
      @backend.stop
    end

    def reset!
      @backend.reset!
    end
  end
end
