require "logger"
require "./sourced"
require "./consumer_groups/consumer"
require "./consumer_groups/events"
require "./consumer_groups/group"

module SourcedStore
  class ConsumerGroups
    ZERO_DURATION = 0.milliseconds

    class GroupProjector < Sourced::Projector(Group)
      on Events::ConsumerCheckedIn do |entity, evt|
        entity.register(evt.payload.consumer_id, evt.timestamp, evt.payload.debounce)
      end

      on Events::ConsumerAcknowledged do |entity, evt|
        entity.ack(evt.payload.consumer_id, evt.payload.last_seq, evt.timestamp)
      end

      on Events::GroupRebalancedAt do |entity, evt|
        entity.rebalance_at(evt.payload.last_seq)
      end
    end

    class GroupStage < Sourced::Stage(Group, GroupProjector)
      def group
        entity
      end
    end

    getter logger : Logger
    getter groups : Hash(String, Group)
    getter liveness_span : Time::Span

    def initialize(@store : Sourced::Store, @liveness_span : Time::Span, @logger : Logger)
      @groups = Hash(String, Group).new { |h, k| h[k] = Group.new(k, @liveness_span) }
    end

    def reset!
      @store.reset!
      @groups = @groups.clear
    end

    def checkin(group_name : String, consumer_id : String, debounce : Time::Span = ZERO_DURATION, last_seq : Sourced::Event::Seq | Nil = nil) : Consumer
      stage = load(group_name)

      last_active_count = stage.group.consumers.size
      min_seq = stage.group.min_seq
      stage.apply(Events::ConsumerCheckedIn.new(consumer_id: consumer_id, debounce: debounce))

      if last_seq
        ack_consumer(stage, consumer_id, last_seq)
      end

      if last_active_count != stage.group.consumers.size && stage.group.any_consumer_not_at?(min_seq) # consumers have been added or removed
        logger.info "[#{group_name}] rebalancing all consumers at #{min_seq}"
        stage.apply(Events::GroupRebalancedAt.new(last_seq: min_seq))
      end

      save(group_name, stage)
      stage.group.consumer_for(consumer_id)
      # rescue ConcurrencyError # TODO
      # reload, re-apply, retry?
    end

    def ack(group_name : String, consumer_id : String, last_seq : Sourced::Event::Seq) : Bool
      stage = load(group_name)
      ack_consumer(stage, consumer_id, last_seq)
      save(group_name, stage)
      true
    end

    def get_consumer(group_name : String, consumer_id : String) : Consumer
      groups[group_name].consumers[consumer_id]
    end

    def load(group_name : String) : GroupStage
      group = groups[group_name]
      GroupStage.new(group_name, group, GroupProjector.new, @store.read_stream(group_name, group.seq))
    end

    private def ack_consumer(stage : GroupStage, consumer_id : String, last_seq : Sourced::Event::Seq)
      if stage.group.has_consumer?(consumer_id)
        stage.apply(Events::ConsumerAcknowledged.new(consumer_id: consumer_id, last_seq: last_seq))
      end
    end

    private def save(group_name : String, stage : Sourced::Stage)
      # TODO: , stage.last_committed_seq)
      if stage.uncommitted_events.any?
        @store.append_to_stream(group_name, stage.uncommitted_events)
      end
    end
  end
end
