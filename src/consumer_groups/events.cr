module SourcedStore
  class ConsumerGroups
    module Events
      include Sourced::Macros

      event ConsumerCheckedIn, "groups.consumer_checked_in", consumer_id : String, debounce : Time::Span = ZERO_DURATION
      event ConsumerAcknowledged, "groups.consumer_acknowledged", consumer_id : String, last_seq : Sourced::Event::Seq
      event GroupRebalancedAt, "groups.rebalanced_at", last_seq : Sourced::Event::Seq

      # Register events for DB de-serialization
      # This has to be done as a separate step
      # because I haven't figured it out yet.
      class Registry
        include Sourced::EventRegistry

        register_events(
          ConsumerCheckedIn,
          ConsumerAcknowledged,
          GroupRebalancedAt,
        )
      end
    end
  end
end
