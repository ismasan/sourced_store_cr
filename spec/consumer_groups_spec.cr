require "logger"
require "./spec_helper"
require "../src/consumer_groups"

describe SourcedStore::ConsumerGroups do
  logger = Logger.new(STDOUT, level: Logger::INFO)
  groups = uninitialized SourcedStore::ConsumerGroups
  store = uninitialized Sourced::MemStore

  before_each do
    store = Sourced::MemStore.new
    groups = SourcedStore::ConsumerGroups.new(
      store: store,
      liveness_span: 10.seconds,
      logger: logger
    )
  end

  describe "#checkin" do
    it "checks in consumer, incrementing their position and group_size" do
      g1c1a = groups.checkin("g1", "c1")
      g1c2a = groups.checkin("g1", "c2")
      g2c1a = groups.checkin("g2", "c1")
      g1c1b = groups.checkin("g1", "c1")

      g1c1a.position.should eq(0)
      g1c1a.last_seq.should eq(0)
      g1c1a.group_size.should eq(1)

      g1c1b.position.should eq(0)
      g1c1b.group_size.should eq(2)

      g1c2a.position.should eq(1)
      g1c2a.group_size.should eq(2)

      g1_stream = store.read_stream("g1")
      g1_stream.map(&.seq).should eq([1, 2, 3])

      g2_stream = store.read_stream("g2")
      g2_stream.map(&.seq).should eq([1])
    end

    it "rebalances group to minimum seq" do
      now = Time.utc
      store.append_to_stream(
        "g1",
        [
          checkin_event(
            now - 30.seconds, # inactive
            1,
            consumer_id: "c1"
          ),
          checkin_event(
            now - 17.seconds, # inactive
            3,
            consumer_id: "c2"
          ),
          checkin_event(
            now - 7.seconds, # active
            5,
            consumer_id: "c3"
          ),
          ack_event(
            now - 5.seconds, # active
            6,
            consumer_id: "c3",
            last_seq: 4
          ),
          checkin_event(
            now - 4.seconds, # active
            7,
            consumer_id: "c4"
          ),
          ack_event(
            now - 2.seconds, # active
            8,
            consumer_id: "c4",
            last_seq: 5
          ),
        ] of Sourced::Event
      )

      cn = groups.checkin("g1", "c5")
      cn.position.should eq(2)
      cn.group_size.should eq(3)
      cn.last_seq.should eq(4) # was rebalanced to least of active seqs

      stream = store.read_stream("g1")
      stream.size.should eq(8)
      stream.last.should be_a(SourcedStore::ConsumerGroups::Events::GroupRebalancedAt)
    end

    it "keeps track of group's minimum seq even when all consumers are inactive" do
      now = Time.utc
      store.append_to_stream(
        "g1",
        [
          checkin_event(
            now - 60.seconds,
            1,
            consumer_id: "c1"
          ),
          ack_event(
            now - 56.seconds,
            2,
            consumer_id: "c1",
            last_seq: 4
          ),
          checkin_event(
            now - 47.seconds,
            3,
            consumer_id: "c2"
          ),
          ack_event(
            now - 47.seconds,
            4,
            consumer_id: "c2",
            last_seq: 14
          ),
        ] of Sourced::Event
      )

      cn = groups.checkin("g1", "c2")
      cn.position.should eq(0)
      cn.group_size.should eq(1)
      cn.last_seq.should eq(4) # was rebalanced to least of active seqs
    end

    it "optionally sets #run_at to arbitrary date in future, while maintaining rebalancing logic" do
      now = Time.utc
      time_format = "%Y-%m-%d : %H:%M:%S"
      store.append_to_stream(
        "g1",
        [
          checkin_event(
            now - 15.seconds,
            1,
            consumer_id: "c1"
          ),
          ack_event(
            now - 9.seconds,
            2,
            consumer_id: "c1",
            last_seq: 4
          ),
          checkin_event( # new consumer. Starts at group's min_seq
            now - 8.seconds,
            3,
            consumer_id: "c2"
          ),
        ] of Sourced::Event
      )

      c2 = groups.checkin("g1", "c2", 10.seconds)
      c2.position.should eq(1)
      c2.group_size.should eq(2)
      c2.last_seq.should eq(4)
      c2.run_at.to_s(time_format).should eq((now + 10.seconds).to_s(time_format))
    end
  end

  describe "#ack" do
    it "updates consumer's #last_seq and commits event" do
      groups.checkin("g1", "c1")
      groups.ack("g1", "c1", 10).should be_true
      cn = groups.get_consumer("g1", "c1")
      cn.last_seq.should eq(10)
      g1_stream = store.read_stream("g1")
      g1_stream.size.should eq(2)
      g1_stream[0].should be_a(SourcedStore::ConsumerGroups::Events::ConsumerCheckedIn)
      g1_stream[1].should be_a(SourcedStore::ConsumerGroups::Events::ConsumerAcknowledged)
    end

    it "is a noop if consumer doesn't exist in group" do
      groups.ack("g1", "c1", 10).should be_true
      g1_stream = store.read_stream("g1")
      g1_stream.size.should eq(0)
    end

    it "updates consumer's #run_at" do
      # a history of previous checkins
      time_ago = Time.utc - 10.seconds
      store.append_to_stream(
        "g1",
        checkin_event(
          time_ago,
          1,
          consumer_id: "c1"
        )
      )

      g1 = groups.load("g1").group
      cn1 = g1.consumers["c1"]
      cn1.run_at.should eq(time_ago)

      groups.ack("g1", "c1", 10)

      cn2 = g1.consumers["c1"]
      cn2.run_at.should_not eq(time_ago)
    end
  end
end

private def checkin_event(time : Time, seq : Int32, consumer_id : String)
  SourcedStore::ConsumerGroups::Events::ConsumerCheckedIn.new(
    time,
    Int64.new(seq),
    SourcedStore::ConsumerGroups::Events::ConsumerCheckedIn::Payload.new(
      consumer_id: consumer_id
    )
  )
end

private def ack_event(time : Time, seq : Int32, consumer_id : String, last_seq : Int32)
  SourcedStore::ConsumerGroups::Events::ConsumerAcknowledged.new(
    time,
    Int64.new(seq),
    SourcedStore::ConsumerGroups::Events::ConsumerAcknowledged::Payload.new(
      consumer_id: consumer_id,
      last_seq: Int64.new(last_seq)
    )
  )
end
