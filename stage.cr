module EventSourcing
  class Event
    alias Seq = Int64
    getter seq : Seq = Int64.new(0)
  end

  alias EventList = Array(Event)

  macro event(class_name, *properties)
    class {{class_name}} < EventSourcing::Event
      {% for property in properties %}
        getter {{property}}
      {% end %}

      def initialize(@seq, {{
         *properties.map do |field|
           "@#{field.id}".id
         end
       }})
      end

      def initialize({{
         *properties.map do |field|
           "@#{field.id}".id
         end
       }})
      end

      def with_seq(seq : EventSourcing::Event::Seq)
        self.class.new(seq, {{
         *properties.map do |field|
           "#{field.var.id}".id
         end
        }})
      end
    end
  end

  class Projector(T)
    def call(entity : T, evt : EventSourcing::Event) : T
      _apply(entity, evt)
      entity
    end

    def _apply(entity : T, evt : EventSourcing::Event)
      puts "Unhandled event #{evt.inspect}"
    end
  end

  class Stage(T, P)
    getter stream_id : String
    getter entity : T
    getter seq : EventSourcing::Event::Seq
    getter last_committed_seq : EventSourcing::Event::Seq
    getter uncommitted_events : Array(EventSourcing::Event)
    getter projector : P

    # Load from stream
    def initialize(stream_id : String, entity : T, projector : P, stream : EventSourcing::EventList)
      seq = Int64.new(0)
      stream.each do |evt|
        entity = projector.call(entity, evt)
        seq = evt.seq
      end

      initialize(stream_id, entity, projector, seq)
    end

    def initialize(@stream_id : String, @entity : T, @projector : P, @seq : EventSourcing::Event::Seq = Int64.new(0))
      @last_committed_seq = @seq
      @uncommitted_events = Array(EventSourcing::Event).new
    end

    def apply(evt : EventSourcing::Event)
      evt = evt.with_seq(seq + 1)
      @entity = projector.call(entity, evt)
      @seq = evt.seq
      @uncommitted_events << evt
    end
  end
end

class User
  property name : String = ""
  property age : Int32 = 0
end

EventSourcing.event NameUpdated, new_name : String
EventSourcing.event AgeUpdated, new_age : Int32

class UserProjector < EventSourcing::Projector(User)
  def _apply(user : User, evt : NameUpdated)
    user.name = evt.new_name
  end

  def _apply(user : User, evt : AgeUpdated)
    user.age = evt.new_age
  end
end

class UserStage < EventSourcing::Stage(User, UserProjector)
end

stream = [
  NameUpdated.new(new_name: "Joe")
] of EventSourcing::Event

user = User.new
# stage = UserStage.new("g1", user, UserProjector.new)
stage = UserStage.new("g1", user, UserProjector.new, stream)
# stage.apply(NameUpdated.new(new_name: "Ismael"))
stage.apply(AgeUpdated.new(new_age: 44))
puts stage.inspect
