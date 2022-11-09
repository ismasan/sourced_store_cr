require "db"
require "./twirp_transport/twirp.pb.cr"

module SourcedStore
  class JSONToBytes
    def self.from_rs(rs : DB::ResultSet) : Bytes?
      cnt = rs.read
      case cnt
      when JSON::PullParser
        any = JSON::Any.new(pull: cnt)
        any.to_json.to_slice
      when JSON::Any
        cnt.to_json.to_slice
      when Nil
        nil
      else
        raise cnt.inspect
      end
    end
  end

  struct EventRecord
    include DB::Serializable
    getter id : UUID
    getter topic : String
    getter stream_id : String
    getter global_seq : Int64
    getter seq : Int32
    getter created_at : Time
    # Converters must be fully qualified modules
    @[DB::Field(key: "metadata", converter: SourcedStore::JSONToBytes)]
    getter metadata : Bytes | Nil
    @[DB::Field(key: "payload", converter: SourcedStore::JSONToBytes)]
    getter payload : Bytes | Nil

    def self.from_proto(evt : TwirpTransport::Event) : EventRecord
      EventRecord.new(
        id: UUID.new(evt.id.as(String)),
        topic: evt.topic.as(String),
        stream_id: evt.stream_id.as(String),
        global_seq: Int64.new(0),
        seq: evt.seq.as(Int32),
        created_at: protobuf_timestamp_to_time(evt.created_at),
        metadata: evt.metadata,
        payload: evt.payload
      )
    end

    def self.protobuf_timestamp_to_time(pbtime : Google::Protobuf::Timestamp | Nil) : Time
      pbtime = pbtime.as(Google::Protobuf::Timestamp)
      span = Time::Span.new(
        seconds: pbtime.seconds.as(Int64),
        nanoseconds: pbtime.nanos.as(Int32)
      )
      Time::UNIX_EPOCH + span
    end

    def initialize(@id, @topic, @stream_id, @global_seq, @seq, @created_at, @metadata, @payload)

    end

    def metadata_bytes : Bytes | Nil
      metadata.is_a?(Nil) ? nil : metadata.to_json.to_slice
    end

    def payload_bytes : Bytes | Nil
      payload.is_a?(Nil) ? nil : payload.to_json.to_slice
    end

    def to_proto : SourcedStore::TwirpTransport::Event
      TwirpTransport::Event.new(
        id: id.to_s,
        topic: topic,
        stream_id: stream_id,
        global_seq: global_seq,
        seq: seq,
        created_at: time_to_protobuf_timestamp(created_at),
        metadata: metadata,
        payload: payload
      )
    end

    private def time_to_protobuf_timestamp(time : Time)
      Google::Protobuf::Timestamp
      span = time - Time::UNIX_EPOCH
      Google::Protobuf::Timestamp.new(
        seconds: span.total_seconds.to_i,
        nanos: span.nanoseconds
      )
    end
  end

  struct Category
    include DB::Serializable
    getter id : UUID
    getter name : String
  end

  record Error, code : String, message : String?
  record ResultWithError, successful : Bool, error : Error?
end
