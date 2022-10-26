module Sourced
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
    macro event(class_name, topic, *properties)
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

        TOPIC = "{{topic.id}}"

        getter payload : Payload

        def self.topic : String
          TOPIC
        end

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

        def initialize(@timestamp, @seq, @payload : Payload)

        end

        def with_seq(seq : Sourced::Event::Seq)
          self.class.new(seq, payload)
        end

        def payload_json : String
          payload.to_json
        end
      end
    end
  end
end
