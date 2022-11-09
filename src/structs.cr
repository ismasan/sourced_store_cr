require "./twirp_transport/twirp.pb.cr"

module SourcedStore
  struct EventRecord
    include DB::Serializable
    getter id : UUID
    getter topic : String
    getter stream_id : String
    getter global_seq : Int64
    getter seq : Int32
    getter created_at : Time
    getter metadata : JSON::Any | Nil
    getter payload : JSON::Any | Nil

    def metadata_bytes
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
        metadata: metadata_bytes,
        payload: payload_bytes
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
end
