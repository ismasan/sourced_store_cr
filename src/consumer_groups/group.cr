module SourcedStore
  class ConsumerGroups
    class Group
      getter name : String

      def initialize(@name)
        @consumers = Hash(String, Consumer).new
        @count = 0
      end

      def info
        %([#{name}: #{size} consumers])
      end

      def register(consumer_id : String)
        consumer = @consumers[consumer_id]?
        return consumer if consumer

        seq = last_global_seq
        consumer = Consumer.new(
          group: self,
          id: consumer_id,
          number: @count,
          last_global_seq: seq
        )
        @consumers[consumer_id] = consumer
        rebalance
        @count += 1
        consumer
      end

      def remove(consumer_id : String)
        return unless @consumers.has_key?(consumer_id)

        cn = @consumers.delete(consumer_id)
        return unless cn
        rebalance
        @count = @consumers.values.size
      end

      private def rebalance
        seq = minimum_global_seq
        num = 0
        @consumers.values.each do |c|
          c.rebalance(num, seq)
          num += 1
        end
      end

      def minimum_global_seq : Int64
        return ZERO64 unless @consumers.any?

        @consumers.values.sort_by { |c| c.last_global_seq }.first.last_global_seq
      end

      def last_global_seq : Int64
        return ZERO64 unless @consumers.any?

        @consumers.values.sort_by { |c| c.last_global_seq }.last.last_global_seq
      end

      def size
        @count
      end
    end
  end
end
