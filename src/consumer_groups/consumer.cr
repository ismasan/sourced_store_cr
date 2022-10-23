module SourcedStore
  class ConsumerGroups
    record Consumer,
      id : String,
      position : Int32,
      group_size : Int32,
      run_at : Time,
      last_seq : Sourced::Event::Seq
  end
end
