module Sourced
  class Event
    alias Seq = Int64
    ZERO_SEQ = Seq.new(0)
    getter seq : Seq = ZERO_SEQ
    getter timestamp : Time = Time.utc

    def self.topic : String
      ""
    end

    def topic : String
      self.class.topic
    end

    class Payload

    end
  end

  alias EventList = Array(Event)
end
