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

    macro event_registry(*event_classes)
      def self.resolve_class(type : String)
        case type
          {% for kls in event_classes %}
            when "{{kls.id}}"
              {{kls.id}}
          {% end %}
        else
          raise "No event class found for '#{type}'"
        end
      end

      def self.build_event(topic : String, seq : Int64, timestamp : Time, payload_json : String)
        case topic
          {% for kls in event_classes %}
            when "{{kls.id}}"
              payload = {{kls.id}}::Payload.from_json(payload_json)
              {{kls.id}}.new(timestamp, seq, payload)
          {% end %}
        else
          raise "No event class found for '#{topic}'"
        end
      end

      def self.from_rs(rs : ::DB::ResultSet)
        events = Array(::Sourced::Event).new

        rs.each do
          topic = rs.read.as(String)
          seq = rs.read.as(Int64)
          timestamp = rs.read.as(Time)
          payload_json = rs.read.as(JSON::PullParser).read_raw

          events << build_event(topic, seq, timestamp, payload_json)
        end

        events
      ensure
        rs.close
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

        getter payload : Payload

        def self.topic : String
          "{{topic.id}}"
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
      end
    end
  end
end
