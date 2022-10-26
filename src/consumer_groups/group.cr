require "./consumer"

module SourcedStore
  class ConsumerGroups
    class Group
      include JSON::Serializable

      alias ConsumerHash = Hash(String, Consumer)

      property seq : Sourced::Event::Seq = Int64.new(0)
      getter name : String
      getter consumers : ConsumerHash
      property events_since_snapshot : Int32 = 0

      def initialize(@name, @liveness_span : Time::Span)
        @consumers = ConsumerHash.new
      end

      def register(id : String, time : Time, debounce : Time::Span = ZERO_DURATION)
        @events_since_snapshot += 1
        run_at = time + debounce
        cn = if has_consumer?(id)
               consumers[id].copy_with(run_at: run_at)
             else
               Consumer.new(id, time, name, 0, 1, run_at, min_seq)
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
        ordered = consumers.values.sort_by(&.registered_at)
        tup = ordered.each_with_index.find { |c, _| c.id == id }
        raise "no consumer for #{id} in #{ordered.map(&.id)}" unless tup
        cn, position = tup

        cn.copy_with(position: position, group_size: ordered.size)
      end

      def min_seq : Sourced::Event::Seq
        seqs = consumers.values.map(&.last_seq)
        seqs.any? ? seqs.min : Sourced::Event::ZERO_SEQ
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
  end
end
