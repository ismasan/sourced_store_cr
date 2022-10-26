module Sourced
  class Projector(T)
    include Macros

    def call(entity : T, evt : Sourced::Event) : T
      _apply(entity, evt)
    end

    def _apply(entity : T, evt : Sourced::Event)
      puts "Unhandled event #{evt.inspect}"
      entity
    end
  end
end
