module Sourced
  class Event
    alias Seq = Int64
    getter seq : Seq = Int64.new(0)

    class Payload

    end
  end

  alias EventList = Array(Event)

  module Macros
    # on NameUpdated do |entity, event|
    #   entity.name = event.payload.name
    # end
    #
    # Produces:
    #
    # def _apply(entity : T, evt : NameUpdated)
    #   entity.name = evt.payload.name
    # end
    macro on(event_class, &block)
      def _apply(entity : T, evt : {{event_class}})
        {{block.body}}
      end
    end

    # event NameUpdated, name : String
    macro event(class_name, *properties)
      class {{class_name}} < Sourced::Event
        class Payload < Sourced::Event::Payload
          {% for property in properties %}
            getter {{property}}
          {% end %}

          def initialize({{
             *properties.map do |field|
               "@#{field.id}".id
             end
           }})
          end
        end

        getter payload : Payload

        def initialize(seq : Sourced::Event::Seq | Int32, {{
           *properties.map do |field|
             "#{field.id}".id
           end
         }})
          @seq = seq.is_a?(Sourced::Event::Seq) ? seq : Int64.new(seq)
          @payload = Payload.new({{
           *properties.map do |field|
             "#{field.var.id}".id
           end
          }})
        end

        def initialize({{
           *properties.map do |field|
             "#{field.id}".id
           end
         }})
          @payload = Payload.new({{
           *properties.map do |field|
             "#{field.var.id}".id
           end
          }})
        end

        def initialize(@seq, @payload : Payload)

        end

        def with_seq(seq : Sourced::Event::Seq)
          self.class.new(seq, payload)
        end
      end
    end
  end

  class Projector(T)
    include Macros

    def call(entity : T, evt : Sourced::Event) : T
      _apply(entity, evt)
      entity
    end

    def _apply(entity : T, evt : Sourced::Event)
      puts "Unhandled event #{evt.inspect}"
    end
  end

  class Stage(T, P)
    getter stream_id : String
    getter entity : T
    getter seq : Sourced::Event::Seq
    getter last_committed_seq : Sourced::Event::Seq
    getter uncommitted_events : Array(Sourced::Event)
    getter projector : P

    # Load from stream
    def initialize(stream_id : String, entity : T, projector : P, stream : Sourced::EventList)
      seq = Int64.new(0)
      stream.each do |evt|
        entity = projector.call(entity, evt)
        seq = evt.seq
      end

      initialize(stream_id, entity, projector, seq)
    end

    def initialize(@stream_id : String, @entity : T, @projector : P, @seq : Sourced::Event::Seq = Int64.new(0))
      @last_committed_seq = @seq
      @uncommitted_events = Array(Sourced::Event).new
    end

    def apply(evt : Sourced::Event)
      evt = evt.with_seq(seq + 1)
      @entity = projector.call(entity, evt)
      @seq = evt.seq
      @uncommitted_events << evt
    end
  end
end

