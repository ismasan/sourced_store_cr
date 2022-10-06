require "logger"
require "timer"

module SourcedStore
  class ConsumerGroups
    ZERO64 = Int64.new(0)
    DEFAULT_LIVENESS_TIMEOUT = 15000 # 15 seconds

    getter liveness_timeout : Time::Span

    def initialize(logger : Logger, liveness_timeout : Int32 = DEFAULT_LIVENESS_TIMEOUT)
      @groups = Hash(String, Group).new
      @lock = Mutex.new
      @logger = logger
      @liveness_timeout = liveness_timeout.milliseconds
      @timers = Hash(String, Timer).new
    end

    def checkin(group_name : String, consumer_id : String)
      register(group_name, consumer_id) do |cn|
        @timers[cn.key].cancel && @timers.delete(cn.key) if @timers.has_key?(cn.key)
        cn.checkin!
      end
    end

    def checkout(consumer : Consumer | Nil) : Consumer | Nil
      return unless consumer

      register(consumer.group_name, consumer.id) do |cn|
        @timers[cn.key] ||= Timer.new(@liveness_timeout) do
          remove_consumer(cn)
        end
        cn.checkout!
      end
    end

    private def remove_consumer(consumer : Consumer)
      @lock.synchronize do
        group = @groups[consumer.group_name]?
        return unless group
        group.remove(consumer.id)
        @groups.delete(consumer.group_name) if group.size == 0
        @logger.info info
      end
    end

    def info
      %(#{@groups.values.size} consumer groups: #{@groups.values.map { |g| g.info }.join(", ")})
    end

    def register(group_name : String, consumer_id : String, &block)
      @lock.synchronize do
        group = @groups[group_name]? || Group.new(name: group_name)
        consumer = group.register(consumer_id)
        @groups[group_name] = group
        yield consumer
      end
    end

    def register(group_name : String, consumer_id : String) : Consumer
      register(group_name, consumer_id) { |cn| cn }
    end

    def notify_consumer(consumer : Consumer, last_global_seq : Int64) : Consumer
      register(consumer.group_name, consumer.id) do |cn|
        cn.notify(last_global_seq)
        cn
      end
    end

    def minimum_global_seq_for(group_name : String) : Int64
      @lock.synchronize do
        group = @groups[group_name]?
        group ? group.minimum_global_seq : ZERO64
      end
    end

    def last_global_seq_for(group_name : String) : Int64
      @lock.synchronize do
        group = @groups[group_name]?
        group ? group.last_global_seq : ZERO64
      end
    end
  end
end

require "./consumer_groups/group"
require "./consumer_groups/consumer"
