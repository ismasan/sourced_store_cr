module Sourced
  class MemStore
    include Store

    def initialize
      @streams = Hash(String, EventList).new { |h, k| h[k] = EventList.new }
      @seqs_index = Hash(String, Int32).new
      @lock = Mutex.new
    end

    def reset! : Bool
      @lock.synchronize do
        @streams = @streams.clear
        @seqs_index = Hash(String, Int32).new
      end
      true
    end

    def read_stream(stream_id : String, after_seq : Event::Seq | Nil = nil, snapshot_topic : String = "") : EventList
      after_seq ||= Event::ZERO_SEQ
      @lock.synchronize do
        events = @streams[stream_id].select { |evt| evt.seq > after_seq }
        events
      end
    end

    def append_to_stream(stream_id : String, events : EventList) : Bool
      guard_concurrent_writes(stream_id, events)

      @lock.synchronize do
        @streams[stream_id] += events
      end

      true
    end

    private def guard_concurrent_writes(stream_id : String, events : EventList)
      events.each do |evt|
        key = "#{stream_id}:#{evt.seq}"
        raise Errors::ConcurrencyError.new("concurrent write on #{key}") if @seqs_index.has_key?(key)
        @seqs_index[key] = 1
      end
    end
  end
end
