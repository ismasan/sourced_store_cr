module Sourced
  class MemStore
    include Store

    def initialize
      @streams = Hash(String, EventList).new { |h, k| h[k] = EventList.new }
      @lock = Mutex.new
    end

    def read_stream(stream_id : String, from_seq : Event::Seq | Nil = nil) : EventList
      from_seq ||= Int64.new(0)
      @lock.synchronize do
        @streams[stream_id].select { |evt| evt.seq > from_seq }
      end
    end

    def append_to_stream(stream_id : String, events : EventList) : Bool
      @lock.synchronize do
        @streams[stream_id] += events
      end

      true
    end
  end
end
