module SourcedStore
  class ConsumerGroups
    class Consumer
      getter group_name : String
      getter id : String
      getter key : String
      getter number : Int32
      getter last_global_seq : Int64
      getter checked_in : Bool

      def initialize(group : Group, @id, @number, last_global_seq : Int64 = ZERO64)
        @group = group
        @group_name = group.name
        @last_global_seq = last_global_seq
        @checked_in = false
        @key = "#{@group_name}:#{@id}"
      end

      def rebalance(n : Int32, seq : Int64)
        @number = n
        @last_global_seq = seq
      end

      def checkin! : Consumer
        @checked_in = true
        self
      end

      def checkout! : Consumer
        @checked_in = false
        self
      end

      def group_size : Int32
        @group.size
      end

      def notify(seq : Int64) : Consumer
        @last_global_seq = seq
        self
      end

      def info : String
        "consumer #{group_name}(#{group_size}):#{id}##{number} on #{last_global_seq}"
      end
    end
  end
end
