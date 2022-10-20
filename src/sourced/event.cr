module Sourced
  class Event
    alias Seq = Int64
    getter seq : Seq = Int64.new(0)
    getter timestamp : Time = Time.utc

    class Payload

    end
  end

  alias EventList = Array(Event)
end
