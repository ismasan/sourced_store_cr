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
    event ConsumerAcknowledged, consumer_id : String, last_seq : Sourced::Event::Seq
    event GroupRebalancedAt, last_seq : Sourced::Event::Seq

    record Consumer,
      id : String,
      position : Int32,
      group_size : Int32,
      run_at : Time,
      last_seq : Sourced::Event::Seq

    class Group
      alias ConsumerHash = Hash(String, Consumer)

      property seq : Sourced::Event::Seq = Int64.new(0)
      getter name : String
      getter consumers : ConsumerHash

      def initialize(@name, @liveness_span : Time::Span)
        @consumers = ConsumerHash.new
        # @consumers = ConsumerHash.new { |h, k|
        #   h[k] = Consumer.new(k, 0, 1, Time.utc, ZERO64)
        # }
      end

      def register(id : String, time : Time)
        cn = if has_consumer?(id)
               consumers[id].copy_with(run_at: time)
             else
               Consumer.new(id, 0, 1, time, ZERO64)
             end
        @consumers[id] = cn
        @consumers = update_liveness_window(@consumers, time)
      end

      def ack(id : String, last_seq : Sourced::Event::Seq, time : Time) : Bool
        return false unless has_consumer?(id)

        cn = @consumers[id].copy_with(run_at: time, last_seq: last_seq)
        @consumers[id] = cn
        true
      end

      def has_consumer?(consumer_id : String) : Bool
        consumers.has_key?(consumer_id)
      end

      # A Consumer with :group_size and :position
      def consumer_for(id : String) : Consumer
        ordered = consumers.values.sort_by(&.id)
        tup = ordered.each_with_index.find { |c, _| c.id == id }
        raise "no consumer for #{id} in #{ordered.map(&.id)}" unless tup
        cn, position = tup

        cn.copy_with(position: position, group_size: ordered.size)
      end

      def min_seq : Sourced::Event::Seq
        seqs = consumers.values.map(&.last_seq)
        seqs.any? ? seqs.min : ZERO64
      end

      def update_liveness_window(cns : ConsumerHash, time : Time) : ConsumerHash
        threshold = time - @liveness_span
        cns.each_with_object(ConsumerHash.new) do |(k, cn), ret|
          ret[k] = cn if cn.run_at >= threshold
        end
      end

      def any_consumer_not_at?(seq : Sourced::Event::Seq) : Bool
        consumers.values.any? { |cn| cn.last_seq != seq }
      end

      def rebalance_at(last_seq : Sourced::Event::Seq)
        @consumers = @consumers.transform_values { |cn| cn.copy_with(last_seq: last_seq) }
      end
    end

    class GroupProjector < Sourced::Projector(Group)
      on ConsumerCheckedIn do |entity, evt|
        entity.register(evt.payload.consumer_id, evt.timestamp)
      end

      on ConsumerAcknowledged do |entity, evt|
        entity.ack(evt.payload.consumer_id, evt.payload.last_seq, evt.timestamp)
      end

      on GroupRebalancedAt do |entity, evt|
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

    def checkin(group_name : String, consumer_id : String) : Consumer
      stage = load(group_name)

      last_active_count = stage.group.consumers.size
      min_seq = stage.group.min_seq
      stage.apply(ConsumerCheckedIn.new(consumer_id: consumer_id))

      if last_active_count != stage.group.consumers.size && stage.group.any_consumer_not_at?(min_seq) # consumers have been added or removed
        stage.apply(GroupRebalancedAt.new(last_seq: min_seq))
      end

      save(group_name, stage)
      stage.group.consumer_for(consumer_id)
    # rescue ConcurrencyError # TODO
      # reload, re-apply, retry?
    end

    def ack(group_name : String, consumer_id : String, last_seq : Sourced::Event::Seq) : Bool
      stage = load(group_name)
      if stage.group.has_consumer?(consumer_id)
        stage.apply(ConsumerAcknowledged.new(consumer_id: consumer_id, last_seq: last_seq))
      end
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

    private def save(group_name : String, stage : Sourced::Stage)
      # TODO: , stage.last_committed_seq)
      if stage.uncommitted_events.any?
        @store.append_to_stream(group_name, stage.uncommitted_events)
      end
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
