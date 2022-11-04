module Sourced
  module Store
    abstract def read_stream(stream_id : String, after_seq : Event::Seq | Nil = nil, snapshot_topic : String = "") : EventList
    abstract def append_to_stream(stream_id : String, events : EventList) : Bool
    abstract def reset! : Bool
    abstract def compact_streams!(snapshot_topic : String, snapshots_to_keep : Int32 = 1) : Bool

    def append_to_stream(stream_id : String, event : Event) : Bool
      events : EventList = [event.as(Event)]
      append_to_stream(stream_id: stream_id, events: events)
    end
  end
end
