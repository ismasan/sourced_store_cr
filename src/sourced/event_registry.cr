require "db"

module Sourced
  module EventRegistry
    abstract def build_event(topic : String, seq : Int64, timestamp : Time, payload_json : String)
    abstract def from_rs(rs : ::DB::ResultSet)

    macro register_events(*event_classes)
      def build_event(topic : String, seq : Int64, timestamp : Time, payload_json : String)
        case topic
          {% for kls in event_classes %}
            when {{kls.id}}::TOPIC
              payload = {{kls.id}}::Payload.from_json(payload_json)
              {{kls.id}}.new(timestamp, seq, payload)
          {% end %}
        else
          raise "No event class found for '#{topic}'"
        end
      end

      def from_rs(rs : ::DB::ResultSet)
        events = Array(::Sourced::Event).new

        rs.each do
          topic = rs.read.as(String)
          seq = rs.read.as(Int64)
          timestamp = rs.read.as(Time)
          payload_json = rs.read.as(JSON::PullParser).read_raw

          events << build_event(topic, seq, timestamp, payload_json)
        end

        events
      ensure
        rs.close
      end
    end
  end
end
