require "logger"
require "./spec_helper"
require "../src/sourced"

module TestApp
  include Sourced::Macros

  # The entity
  class User
    property name : String = ""
    property age : Int32 = 0
  end

  # The things that happen to the entity (events)
  event NameUpdated, new_name : String
  event AgeUpdated, new_age : Int32

  # How events are applied to the entity (projector)
  class UserProjector < Sourced::Projector(User)
    on NameUpdated do |entity, evt|
      entity.name = evt.payload.new_name
    end

    on AgeUpdated do |entity, evt|
      entity.age = evt.payload.new_age
    end
  end

  class UserStage < Sourced::Stage(User, UserProjector)
  end
end

describe Sourced do
  stream = [
    TestApp::NameUpdated.new(seq: 1, new_name: "Joe"),
    TestApp::AgeUpdated.new(seq: 2, new_age: 30),
  ] of Sourced::Event

  it "applies event stream to entity" do
    user = TestApp::User.new
    stage = TestApp::UserStage.new("g1", user, TestApp::UserProjector.new, stream)
    stage.seq.should eq(2)
    stage.last_committed_seq.should eq(2)
    stage.entity.should be_a(TestApp::User)
    stage.entity.name.should eq("Joe")
    stage.entity.age.should eq(30)
    stage.uncommitted_events.size.should eq(0)
  end

  it "applies new events and collect uncommitted events with the right sequence number" do
    user = TestApp::User.new
    stage = TestApp::UserStage.new("g1", user, TestApp::UserProjector.new, stream)
    stage.apply(TestApp::NameUpdated.new(new_name: "Ismael"))
    stage.apply(TestApp::AgeUpdated.new(new_age: 44))
    stage.seq.should eq(4)
    stage.last_committed_seq.should eq(2)
    stage.entity.name.should eq("Ismael")
    stage.entity.age.should eq(44)
    stage.uncommitted_events.size.should eq(2)
    stage.uncommitted_events[0].should be_a(TestApp::NameUpdated)
    stage.uncommitted_events[0].seq.should eq(3)
    stage.uncommitted_events[1].should be_a(TestApp::AgeUpdated)
    stage.uncommitted_events[1].seq.should eq(4)
  end
end
