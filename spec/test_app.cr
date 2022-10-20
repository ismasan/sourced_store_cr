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

