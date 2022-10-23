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
      stream.last.should be_a(SourcedStore::ConsumerGroups::GroupRebalancedAt)
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
      g1_stream[0].should be_a(SourcedStore::ConsumerGroups::ConsumerCheckedIn)
      g1_stream[1].should be_a(SourcedStore::ConsumerGroups::ConsumerAcknowledged)
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
  SourcedStore::ConsumerGroups::ConsumerCheckedIn.new(
    time,
    Int64.new(seq),
    SourcedStore::ConsumerGroups::ConsumerCheckedIn::Payload.new(
      consumer_id: consumer_id
    )
  )
end

private def ack_event(time : Time, seq : Int32, consumer_id : String, last_seq : Int32)
  SourcedStore::ConsumerGroups::ConsumerAcknowledged.new(
    time,
    Int64.new(seq),
    SourcedStore::ConsumerGroups::ConsumerAcknowledged::Payload.new(
      consumer_id: consumer_id,
      last_seq: Int64.new(last_seq)
    )
  )
end
