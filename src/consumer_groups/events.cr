module SourcedStore
  class ConsumerGroups
    module Events
      include Sourced::Macros

      event ConsumerCheckedIn, consumer_id : String
      event ConsumerAcknowledged, consumer_id : String, last_seq : Sourced::Event::Seq
      event GroupRebalancedAt, last_seq : Sourced::Event::Seq
    end
  end
end
