require "./../spec_helper"
require "./../test_app"
require "./../../src/consumer_groups/pg_store"

describe SourcedStore::ConsumerGroups::PGStore do
  test_db_url = "postgres://localhost/sourced_store_test"
  store = uninitialized SourcedStore::ConsumerGroups::PGStore
  db = uninitialized DB::Database
  logger = Logger.new(STDOUT, level: Logger::INFO)
  test_event_registry = TestApp::Registry.new

  before_all do
    db = DB.open(test_db_url)
    CLI::Setup.run(["--database=#{test_db_url}"])
  end

  before_each do
    store = SourcedStore::ConsumerGroups::PGStore.new(db, logger, test_event_registry)
    store.reset!
  end

  after_all do
    db.close
  end

  describe "#append_to_stream and #read_stream" do
    it "works" do
      events : Sourced::EventList = [
        TestApp::NameUpdated.new(seq: 1, new_name: "Frank"),
        TestApp::AgeUpdated.new(seq: 2, new_age: 34),
      ]

      ret = store.append_to_stream("g1", events)
      ret.should be_true

      more_events : Sourced::EventList = [
        TestApp::AgeUpdated.new(seq: 3, new_age: 35).as(Sourced::Event),
      ]

      store.append_to_stream("g1", more_events)

      events_g2 : Sourced::EventList = [
        TestApp::AgeUpdated.new(seq: 1, new_age: 20).as(Sourced::Event),
      ]

      store.append_to_stream("g2", events_g2)

      read_events_g1 = store.read_stream("g1")
      read_events_g1.should be_a(Sourced::EventList)
      read_events_g1.map(&.seq).should eq([1, 2, 3])

      read_events_g2 = store.read_stream("g2")
      read_events_g2.map(&.seq).should eq([1])

      from_seq = store.read_stream("g1", 1)
      from_seq.map(&.seq).should eq([2, 3])
    end

    it "can read after a given after_seq" do
      events : Sourced::EventList = [
        TestApp::NameUpdated.new(seq: 1, new_name: "Frank"),
        TestApp::AgeUpdated.new(seq: 2, new_age: 34),
        TestApp::NameUpdated.new(seq: 3, new_name: "Joe"),
      ]

      store.append_to_stream("g1", events)
      events = store.read_stream("g1", 1)
      events.map(&.seq).should eq([2, 3])
    end

    it "can read from last snapshot if after_seq is 0" do
      events : Sourced::EventList = [
        TestApp::NameUpdated.new(seq: 1, new_name: "Frank"),
        TestApp::AgeUpdated.new(seq: 2, new_age: 34),
        TestApp::Snapshot.new(seq: 3, name: "Frank", age: 34),
        TestApp::NameUpdated.new(seq: 4, new_name: "Joe"),
      ]

      store.append_to_stream("g1", events)
      # if after_seq is not zero, ignore snapshots
      events = store.read_stream("g1", 1, TestApp::Snapshot.topic)
      events.map(&.seq).should eq([2, 3, 4])

      # if after_seq is 0, read from last snapshot, inclusive
      events = store.read_stream("g1", 0, TestApp::Snapshot.topic)
      events.map(&.seq).should eq([3, 4])
    end

    it "raises on concurrent write errors" do
      events : Sourced::EventList = [
        TestApp::NameUpdated.new(seq: 1, new_name: "Frank"),
        TestApp::AgeUpdated.new(seq: 2, new_age: 34),
      ]
      store.append_to_stream("g1", events)

      more_events = [
        TestApp::AgeUpdated.new(seq: 2, new_age: 35),
      ] of Sourced::Event

      expect_raises(Sourced::Errors::ConcurrencyError) do
        store.append_to_stream("g1", more_events)
      end
    end
  end
end
