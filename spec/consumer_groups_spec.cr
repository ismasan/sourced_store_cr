require "logger"
require "./spec_helper"
require "../src/consumer_groups"

describe SourcedStore::ConsumerGroups do
  logger = Logger.new(STDOUT, level: Logger::INFO)
  groups = uninitialized SourcedStore::ConsumerGroups

  before_each do
    groups = SourcedStore::ConsumerGroups.new(logger)
  end

  describe "#register" do
    it "registers and groups consumers, assigning them numbers" do
      c1 = groups.register("group-1", "c1")
      c2 = groups.register("group-1", "c2")
      c1b = groups.register("group-1", "c1")

      c1.number.should eq(0)
      c1.group_size.should eq(2)
      c2.number.should eq(1)
      c2.group_size.should eq(2)
      c1b.number.should eq(0)
      c1.should eq(c1b)
      c1.group_name.should eq("group-1")
      c2.group_name.should eq("group-1")
    end
  end

  describe "#notify_consumer" do
    it "updates consumers with last_global_seq and keeps track of them" do
      c1 = groups.register("group-1", "c1")
      c2 = groups.register("group-1", "c2")

      groups.notify_consumer(c1, 2)
      groups.notify_consumer(c2, 4)
      groups.notify_consumer(c1, 5)

      c1.last_global_seq.should eq(5)
      c2.last_global_seq.should eq(4)

      groups.minimum_global_seq_for("group-1").should eq(4)
      groups.minimum_global_seq_for("nope").should eq(0)
    end
  end
end
