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
    getter payload : JSON::Any | Nil

    def payload_bytes : Bytes | Nil
      payload.is_a?(Nil) ? nil : payload.to_json.to_slice
    end
  end

  class Service < SourcedStore::TwirpTransport::EventStore
    module ErrorCodes
      CONCURRENT_WRITE_LOCK_ERROR = "concurrent_write_lock_error"
    end

    PG_EVENT_SEQ_INDEX_EXP = /unique_index_on_event_seqs/

    READ_STREAM_SQL = %(select
            id,
            topic,
            stream_id,
            originator_id,
            seq,
            created_at,
            payload
            from event_store.events
            where stream_id = $1
            order by seq ASC)

    INSERT_EVENT_SQL = %(insert into event_store.events
            (id, topic, stream_id, originator_id, seq, created_at, payload)
            values ($1::uuid, $2, $3, $4, $5, $6::timestamp, $7)
    )

    @db : DB::Database

    def initialize(logger : Logger, db_url : String)
      @logger = logger
      @db = DB.open(db_url)
    end

    def read_stream(req : TwirpTransport::ReadStreamRequest) : TwirpTransport::ReadStreamResponse
      @db.query(READ_STREAM_SQL, req.stream_id) do |rs|
        events = EventRecord.from_rs(rs).map do |rec|
          originator_id : String | Nil = rec.originator_id.to_s
          originator_id = nil if originator_id == ""

          TwirpTransport::Event.new(
            id: rec.id.to_s,
            topic: rec.topic,
            stream_id: rec.stream_id,
            originator_id: originator_id,
            seq: rec.seq,
            created_at: time_to_protobuf_timestamp(rec.created_at),
            payload: rec.payload_bytes
          )
        end

        TwirpTransport::ReadStreamResponse.new(events: events)
      end
    end

    def append_to_stream(req : TwirpTransport::AppendToStreamRequest) : TwirpTransport::AppendToStreamResponse
      @logger.info "Appending #{req.events} events to stream '#{req.stream_id}'"
      @db.transaction do |tx|
        conn = tx.connection
        req.events.as(Array(SourcedStore::TwirpTransport::Event)).each do |evt|
          conn.exec(
            INSERT_EVENT_SQL,
            evt.id,
            evt.topic,
            evt.stream_id,
            evt.originator_id,
            evt.seq,
            protobuf_timestamp_to_time(evt.created_at),
            evt.payload
          )
        end
      end

      TwirpTransport::AppendToStreamResponse.new(
        successful: true
      )
    rescue err
      @logger.error err.inspect
      err_code = case err.message
                 when PG_EVENT_SEQ_INDEX_EXP
                   ErrorCodes::CONCURRENT_WRITE_LOCK_ERROR
                 else
                   "error"
                 end
      TwirpTransport::AppendToStreamResponse.new(
        successful: false,
        error: TwirpTransport::Error.new(
          code: err_code,
          message: err.message
        )
      )
    end

    def stop
      @logger.info "CLOSING DB"
      @db.close
    end

    def reset!
      return unless ENV["ENV"] == "test"

      @logger.info "Resetting DB. Careful!"
      @db.exec("DELETE FROM event_store.events")
    end

    private def time_to_protobuf_timestamp(time : Time)
      Google::Protobuf::Timestamp
      span = time - Time::UNIX_EPOCH
      Google::Protobuf::Timestamp.new(
        seconds: span.total_seconds.to_i,
        nanos: span.nanoseconds
      )
    end

    private def protobuf_timestamp_to_time(pbtime : Google::Protobuf::Timestamp | Nil) : Time
      pbtime = pbtime.as(Google::Protobuf::Timestamp)
      span = Time::Span.new(
        seconds: pbtime.seconds.as(Int64),
        nanoseconds: pbtime.nanos.as(Int32)
      )
      Time::UNIX_EPOCH + span
    end
  end
end
