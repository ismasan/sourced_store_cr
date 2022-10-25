require "json"
require "db"

struct Time::Span
  include JSON::Serializable
end

module Sourced
  class Event
    include DB::Serializable

    alias Seq = Int64
    ZERO_SEQ = Seq.new(0)
    getter seq : Seq = Sourced::Event::ZERO_SEQ
    getter timestamp : Time = Time.utc

    def self.topic : String
      ""
    end

    def topic : String
      self.class.topic
    end

    class Payload
      include JSON::Serializable
    end
  end

  alias EventList = Array(Event)
end
