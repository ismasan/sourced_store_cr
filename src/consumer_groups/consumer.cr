module SourcedStore
  class ConsumerGroups
    record Consumer,
      id : String,
      group_name : String,
      position : Int32,
      group_size : Int32,
      run_at : Time,
      last_seq : Sourced::Event::Seq do

      def info
        %([#{id}] pos:#{position} gsize:#{group_size} at:#{last_seq})
      end

      def key
        [group_name, id].join(":")
      end
    end
  end
end
