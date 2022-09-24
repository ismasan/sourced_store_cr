require "twirp"
require "twirp/server"
require "db"
require "pg"

require "./twirp_transport/twirp.twirp.cr"
require "./twirp_transport/twirp.pb.cr"

module SourcedStore
  struct EventRecord
    include DB::Serializable
    getter id : UUID
    getter topic : String
    getter stream_id : String
    getter originator_id : UUID | Nil
    getter seq : Int32
    getter created_at : Time
    getter producer_id : String | Nil
    getter elapsed_time : Int32 | Nil
    getter account_id : String | Nil
    getter user_id : String | Nil
    getter payload : JSON::Any | Nil
    getter stack_id : UUID | Nil

    def payload_bytes
      payload.to_json.to_slice
    end
  end

  class Service < SourcedStore::TwirpTransport::EventStore

    READ_STREAM_QUERY = %(select
            id,
            topic,
            stream_id,
            originator_id,
            seq,
            created_at,
            producer_id,
            elapsed_time,
            account_id,
            user_id,
            payload,
            stack_id
            from events
            where stream_id = $1
            order by seq ASC)

    @db : DB::Database

    def initialize(logger : Logger, db_url : String)
      @logger = logger
      @db = DB.open(db_url)
    end

    def read_stream(req : TwirpTransport::ReadStreamRequest) : TwirpTransport::ReadStreamResponse
      # payload = [TwirpTransport::Event::PayloadEntry.new(key: "name", value: "Ismael")]
      @db.query(READ_STREAM_QUERY, req.stream_id) do |rs|
        events = EventRecord.from_rs(rs).map do |rec|
          TwirpTransport::Event.new(
            topic: rec.topic,
            stream_id: rec.stream_id,
            payload: rec.payload_bytes
          )
        end

        TwirpTransport::ReadStreamResponse.new(events: events)
      end

      # event = TwirpTransport::Event.new(
      #   stream_id: req.stream_id,
      #   topic: "carts.items.added",
      #   payload: "{\"name\":\"Ismael\",\"age\":44}".to_slice
      # )

      # TwirpTransport::ReadStreamResponse.new(
      #   events: [event]
      # )
    end

    def append_to_stream(req : TwirpTransport::AppendToStreamRequest) : TwirpTransport::AppendToStreamResponse
      @logger.info "Appending events to stream '#{req.stream_id}'"
      @logger.info req.events.inspect
      TwirpTransport::AppendToStreamResponse.new(
        successful: true
      )
    end

    def stop
      puts "CLOSING DB"
      @db.close
    end
  end
end
