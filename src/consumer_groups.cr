require "logger"

module SourcedStore
  class ConsumerGroups
    ZERO64 = Int64.new(0)

    class Group
      getter name : String

      def initialize(@name)
        @consumers = Hash(String, Consumer).new
        @count = 0
      end

      def register(consumer_id : String)
        consumer = @consumers[consumer_id]?
        return consumer if consumer

        consumer = Consumer.new(group: self, id: consumer_id, number: @count)
        @consumers[consumer_id] = consumer
        @count += 1
        consumer
      end

      def minimum_global_seq : Int64
        return ZERO64 unless @consumers.any?

        @consumers.values.sort_by { |c| c.last_global_seq }.first.last_global_seq
      end

      def size
        @count
      end
    end

    class Consumer
      getter group_name : String
      getter id : String
      getter number : Int32
      getter last_global_seq : Int64

      def initialize(group : Group, @id, @number)
        @group = group
        @group_name = group.name
        @last_global_seq = ZERO64
      end

      def group_size : Int32
        @group.size
      end

      def notify(seq : Int64) : Consumer
        @last_global_seq = seq
        self
      end
    end

    def initialize(logger : Logger)
      @groups = Hash(String, Group).new
      @lock = Mutex.new
      @logger = logger
    end

    def register(group_name : String, consumer_id : String, &block)
      group = register(group_name, consumer_id)
      yield group
    end

    def register(group_name : String, consumer_id : String) : Consumer
      @lock.synchronize do
        group = @groups[group_name]? || Group.new(name: group_name)
        consumer = group.register(consumer_id)
        @groups[group_name] = group
        consumer
      end
    end

    def notify_consumer(consumer : Consumer, last_global_seq : Int64) : Consumer
      register(consumer.group_name, consumer.id) do |cn|
        cn.notify(last_global_seq)
        @logger.info "consumer #{cn.group_name}/#{cn.id} (#{cn.number}) on #{cn.last_global_seq}"
        cn
      end
    end

    def minimum_global_seq_for(group_name : String) : Int64
      @lock.synchronize do
        group = @groups[group_name]?
        group ? group.minimum_global_seq : ZERO64
      end
    end
  end
end
