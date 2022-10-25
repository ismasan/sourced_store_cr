require "json"
require "db"
require "pg"
require "./src/consumer_groups"

include SourcedStore::ConsumerGroups::Events

record Row, topic : String, seq : Sourced::Event::Seq, timestamp : Time, payload : String

REGISTRY = {} of String => Sourced::Event.class
REGISTRY[ConsumerCheckedIn.topic] = ConsumerCheckedIn

module Sourced
  class Event
    def self.from_row(row : Row)
      payload = Payload.from_json(row.payload)
      new(row.timestamp, row.seq, payload)
    end
  end
end

json = ConsumerCheckedIn.new(
  consumer_id: "c1",
  debounce: 11.seconds,
).payload.to_json

# p ConsumerCheckedIn::Payload.from_json(json)

row = Row.new(
  "groups.consumer_checked_in",
  1,
  Time.utc,
  %("payload":{"consumer_id":"c1","debounce":{"seconds":10,"nanoseconds":0}}})
)

module Foo
  include Sourced::Macros
  event_registry(
    SourcedStore::ConsumerGroups::Events::ConsumerCheckedIn,
    SourcedStore::ConsumerGroups::Events::ConsumerAcknowledged
  )
end

db = DB.open("postgres://localhost/sourced_store_development")
records = db.query(%(SELECT topic, seq, timestamp, payload FROM event_store.internal_events ORDER BY id ASC))
# records.close
# db.close

# events = Array(Sourced::Event).new

p Foo.from_rs(records)
# p Foo.resolve_class("SourcedStore::ConsumerGroups::Events::ConsumerCheckedIn")
# records.each do
#   topic = records.read.as(String)
#   seq = records.read.as(Int64)
#   timestamp = records.read.as(Time)
#   payload_json = records.read.as(JSON::PullParser).read_raw

#   case topic
#   when "groups.consumer_checked_in"
#     payload = ConsumerCheckedIn::Payload.from_json(payload_json)
#     events << ConsumerCheckedIn.new(timestamp, seq, payload)
#   end
# end

# p ConsumerCheckedIn.to_s
# p events

# db.close
