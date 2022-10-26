module Sourced
  class Stage(T, P)
    getter stream_id : String
    getter entity : T
    getter seq : Sourced::Event::Seq
    getter last_committed_seq : Sourced::Event::Seq
    getter uncommitted_events : Array(Sourced::Event)
    getter projector : P

    # Load from stream
    def initialize(stream_id : String, entity : T, projector : P, stream : Sourced::EventList, seq : Sourced::Event::Seq = Int64.new(0))
      stream.each do |evt|
        entity = projector.call(entity, evt)
        seq = evt.seq
      end

      initialize(stream_id, entity, projector, seq)
    end

    def initialize(@stream_id : String, @entity : T, @projector : P, @seq : Sourced::Event::Seq = Int64.new(0))
      @last_committed_seq = @seq
      @uncommitted_events = Array(Sourced::Event).new
    end

    def apply(evt : Sourced::Event)
      evt = evt.with_seq(seq + 1)
      @entity = projector.call(entity, evt)
      @seq = evt.seq
      @uncommitted_events << evt
    end
  end
end
