require "./../spec_helper"
require "./../test_app"
require "./../../src/sourced"

describe Sourced::MemStore do
  store = uninitialized Sourced::MemStore

  before_each do
    store = Sourced::MemStore.new
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
