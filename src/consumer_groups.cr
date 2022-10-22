require "logger"
require "./sourced"
# require "./consumer_groups/events"
# require "./consumer_groups/group"
# require "./consumer_groups/store"
# require "./consumer_groups/mem_store"

module SourcedStore
  class ConsumerGroups
    include Sourced::Macros

    ZERO64 = Int64.new(0)

    event ConsumerCheckedIn, consumer_id : String
    event GroupRebalancedAt, last_seq : Sourced::Event::Seq

    record Consumer, id : String, group_name : String, position : Int32, group_size : Int32

    class ConsumerRecord
      property id : String
      property run_at : Time
      property last_seq : Sourced::Event::Seq

      def initialize(@id : String, @run_at : Time, @last_seq : Sourced::Event::Seq)

      end

      def touch!(time : Time)
        @run_at = time
        self
      end
    end

    class Group
      property seq : Sourced::Event::Seq = Int64.new(0)
      getter name : String
      getter last_active_count : Int32
      getter consumers : Hash(String, ConsumerRecord)

      def initialize(@name, @liveness_span : Time::Span)
        @last_active_count = 0
        @consumers = Hash(String, ConsumerRecord).new { |h, k| h[k] = ConsumerRecord.new(k, Time.utc, ZERO64) }
      end

      def register(id : String, time : Time = Time.utc)
        @consumers[id].touch!(time)
      end

      # A Consumer with :group_size and :position
      def consumer_for(id : String) : Consumer
        records = active_consumers
        tup = records.each_with_index.find { |c, _| c.id == id }
        raise "no consumer for #{id} in #{records.map(&.id)}" unless tup

        record, position = tup
        Consumer.new(
          id: record.id,
          group_name: name,
          position: position,
          group_size: records.size
        )
      end

      def min_seq : Sourced::Event::Seq
        active_consumers.map(&.last_seq).min
      end

      def active_consumers
        threshold = Time.utc - @liveness_span
        @consumers
          .values
          .select { |cn| cn.run_at >= threshold }
          .sort_by(&.id)
      end
    end

    class GroupProjector < Sourced::Projector(Group)
      on ConsumerCheckedIn do |entity, evt|
        entity.register(evt.payload.consumer_id, evt.timestamp)
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

    def checkin(group_name : String, consumer_id : String) : Consumer
      stage = load(group_name)

      last_active_count = stage.group.last_active_count
      stage.apply(ConsumerCheckedIn.new(consumer_id: consumer_id))
      if last_active_count != stage.group.last_active_count # consumers have been added or removed
        stage.apply(GroupRebalancedAt.new(last_seq: stage.group.min_seq))
      end

      save(group_name, stage)
      stage.group.consumer_for(consumer_id)
    # rescue ConcurrencyError # TODO
      # reload, re-apply, retry?
    end

    # def ack(group_name : String, consumer_id : String, last_seq : Sourced::Event::Seq) : Bool
    #   stage = load(group_name)
    #   stage.apply(ConsumerAcknowledged.new(consumer_id: consumer_id, seq: last_seq))
    #   save(group_name, stage)
    #   true
    # end

    private def load(group_name : String) : GroupStage
      group = groups[group_name]
      GroupStage.new(group_name, group, GroupProjector.new, @store.read_stream(group_name, group.seq))
    end

    private def save(group_name : String, stage : Sourced::Stage)
      # TODO: , stage.last_committed_seq)
      @store.append_to_stream(group_name, stage.uncommitted_events)
    end
  end
end

# logger = Logger.new(STDOUT, level: Logger::DEBUG)
# store = Sourced::MemStore.new
# groups = SourcedStore::ConsumerGroups.new(store: store, liveness_span: 10.milliseconds, logger: logger)

# puts (groups.checkin("g1", "c1") do |cn|
#   cn.inspect
# end)
# puts (groups.checkin("g1", "c2") do |cn|
#   cn.inspect
# end)
# puts (groups.checkin("g1", "c1") do |cn|
#   cn.inspect
# end)
# puts (groups.checkin("g2", "c1") do |cn|
#   cn.inspect
# end)
# sleep 1

# puts (groups.checkin("g1", "c1") do |cn|
#   cn.inspect
# end)

# pp store.read_stream("g1")
# pp store.read_stream("g2")
