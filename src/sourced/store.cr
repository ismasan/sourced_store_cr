module Sourced
  module Store
    abstract def read_stream(stream_id : String, from_seq : Event::Seq | Nil = nil) : EventList
    abstract def append_to_stream(stream_id : String, events : EventList) : Bool
    abstract def reset! : Bool

    def append_to_stream(stream_id : String, event : Event) : Bool
      events : EventList = [event.as(Event)]
      append_to_stream(stream_id: stream_id, events: events)
    end
  end
end
