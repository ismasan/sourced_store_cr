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
        entity.seq = evt.seq
        entity.events_since_snapshot += 1
        entity.register(evt.payload.consumer_id, evt.timestamp, evt.payload.debounce)
      end

      on Events::ConsumerAcknowledged do |entity, evt|
        entity.seq = evt.seq
        entity.events_since_snapshot += 1
        entity.ack(evt.payload.consumer_id, evt.payload.last_seq, evt.timestamp)
      end

      on Events::GroupRebalancedAt do |entity, evt|
        entity.seq = evt.seq
        entity.events_since_snapshot += 1
        entity.rebalance_at(evt.payload.last_seq)
      end

      on Events::GroupSnapshot do |entity, evt|
        # replace entity with snapshot
        evt.payload.group.tap do |gr|
          gr.events_since_snapshot = 0
        end
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

    def initialize(@store : Sourced::Store, @liveness_span : Time::Span, @logger : Logger, @snapshot_every : Int32 = 100, @compact_every : Time::Span = 30.minutes)
      @groups = Hash(String, Group).new { |h, k| h[k] = Group.new(k, @liveness_span) }
      spawn do
        while true
          @store.compact_streams!(Events::GroupSnapshot.topic, 1)
          sleep @compact_every
        end
      end
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

      ack_consumer(stage, consumer_id, last_seq) if last_seq

      if last_active_count != stage.group.consumers.size && stage.group.any_consumer_not_at?(min_seq) # consumers have been added or removed
        logger.info "[#{group_name}] rebalancing all consumers at #{min_seq}"
        stage.apply(Events::GroupRebalancedAt.new(last_seq: min_seq))
      end

      if stage.group.events_since_snapshot >= @snapshot_every
        logger.info "[#{group_name}] snapshot"
        stage.apply(Events::GroupSnapshot.new(group: stage.group))
      end

      save(group_name, stage)
      stage.group.consumer_for(consumer_id)
    # rescue Sourced::Errors::ConcurrencyError # TODO
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

      GroupStage.new(
        group_name,
        group,
        GroupProjector.new,
        @store.read_stream(group_name, group.seq, Events::GroupSnapshot.topic),
        group.seq
      )
    end

    private def ack_consumer(stage : GroupStage, consumer_id : String, last_seq : Sourced::Event::Seq)
      stage.group.with_consumer(consumer_id) do |cn|
        if cn.last_seq != last_seq
          stage.apply(Events::ConsumerAcknowledged.new(consumer_id: consumer_id, last_seq: last_seq))
        end
      end
    end

    private def save(group_name : String, stage : Sourced::Stage)
      # TODO: , stage.last_committed_seq)
      if stage.uncommitted_events.any?
        @store.append_to_stream(group_name, stage.uncommitted_events)
      end
      # Make sure to replace local group
      # ex. when getting new group stage from snapshot event
      @groups[group_name] = stage.group
    end
  end
end
