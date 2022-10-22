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
      liveness_span: 10.milliseconds,
      logger: logger
    )
  end

  it "works" do
    g1c1a = groups.checkin("g1", "c1")
    g1c2a = groups.checkin("g1", "c2")
    g1c1b = groups.checkin("g1", "c1")

    g1c1a.position.should eq(0)
    g1c1a.group_size.should eq(1)

    g1c1b.position.should eq(0)
    g1c1b.group_size.should eq(2)

    g1c2a.position.should eq(1)
    g1c2a.group_size.should eq(2)
  end
end
