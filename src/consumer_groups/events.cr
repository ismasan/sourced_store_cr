module SourcedStore
  class ConsumerGroups
    module Events
      include Sourced::Macros

      event ConsumerCheckedIn, "groups.consumer_checked_in", consumer_id : String
      event ConsumerAcknowledged, "groups.consumer_acknowledged", consumer_id : String, last_seq : Sourced::Event::Seq
      event GroupRebalancedAt, "groups.rebalanced_at", last_seq : Sourced::Event::Seq
    end
  end
end
