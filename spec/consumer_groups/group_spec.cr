require "../spec_helper"
require "../../src/consumer_groups/group"

describe SourcedStore::ConsumerGroups::Group do
  it "works" do
    group = SourcedStore::ConsumerGroups::Group.new("test")
    c1 = group.register("c1")
    c2 = group.register("c2")
    c1.group_size.should eq(2)
    c1.number.should eq(0)
    c2.group_size.should eq(2)
    c2.number.should eq(1)
    group.size.should eq(2)

    group.remove(c1.id)
    group.size.should eq(1)
    c2 = group.register("c2")
    c2.number.should eq(0)
  end
end
