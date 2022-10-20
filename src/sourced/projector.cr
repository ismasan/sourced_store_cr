module Sourced
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
end
