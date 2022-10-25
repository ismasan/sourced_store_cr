module Sourced
  class MemStore
    include Store

    def initialize
      @streams = Hash(String, EventList).new { |h, k| h[k] = EventList.new }
      @lock = Mutex.new
    end

    def reset! : Bool
      @lock.synchronize do
        @streams = @streams.clear
      end
      true
    end

    def read_stream(stream_id : String, from_seq : Event::Seq | Nil = nil) : EventList
      from_seq ||= Event::ZERO_SEQ
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
