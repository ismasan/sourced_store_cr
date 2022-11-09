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
  end

  struct Category
    include DB::Serializable
    getter id : UUID
    getter name : String
  end
end
