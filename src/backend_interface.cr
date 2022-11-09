require "./structs"
require "./consumer_groups"

module SourcedStore
  module BackendInterface
    abstract def info : String
    abstract def append_to_stream(stream_id : String, events : Array(EventRecord)) : ResultWithError
    abstract def read_stream(stream_id : String, upto_seq : Int32 | Nil = nil) : Array(EventRecord)
    abstract def read_category(
      category : String,
      consumer_group : String,
      consumer_id : String,
      last_seq : Int64 | Nil,
      batch_size : Int32,
      wait_timeout : Time::Span | Nil
    ) : Array(EventRecord)

    abstract def ack_consumer(consumer_group : String, consumer_id : String, last_seq : Int64?) : Bool
    abstract def stop
    abstract def reset!

    def append_to_stream!(stream_id : String, events : Array(EventRecord)) : ResultWithError
      result = append_to_stream(stream_id, events)
      raise "Could not append: #{result.error.inspect}" unless result.successful

      result
    end

    def ack_consumer(consumer : ConsumerGroups::Consumer, last_seq : Int64) : Bool
      ack_consumer(consumer_group: consumer.group_name, consumer_id: consumer.id, last_seq: last_seq)
    end
  end
end
