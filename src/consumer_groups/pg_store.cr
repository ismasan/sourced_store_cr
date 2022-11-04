module SourcedStore
  class ConsumerGroups
    class PGStore
      include Sourced::Store

      PG_EVENT_SEQ_INDEX_EXP = /unique_index_on_internal_event_seqs/

      READ_STREAM_SQL = %(SELECT
                          topic,
                          seq,
                          timestamp,
                          payload
                          FROM event_store.read_internal_stream($1, $2, $3))

      INSERT_EVENT_SQL = %(INSERT INTO event_store.internal_events
              (stream_id, topic, seq, timestamp, payload)
              values ($1::varchar, $2::varchar, $3, $4::timestamp, $5)
      )

      COMPACT_STREAMS_SQL = %(CALL event_store.compact_streams($1, $2))

      def initialize(@db : DB::Database, @logger : Logger, @registry : Sourced::EventRegistry = SourcedStore::ConsumerGroups::Events::Registry.new)
      end

      def compact_streams!(snapshot_topic : String, snapshots_to_keep : Int32 = 1) : Bool
        @logger.debug { "compact all streams with #{snapshots_to_keep} last '#{snapshot_topic}' snapshots" }
        @db.exec(COMPACT_STREAMS_SQL, snapshot_topic, snapshots_to_keep)
        true
      end

      def reset! : Bool
        @logger.info "Resetting consumer groups store. Careful!"
        @db.exec("TRUNCATE event_store.internal_events RESTART IDENTITY")
        true
      end

      def read_stream(stream_id : String, after_seq : Sourced::Event::Seq | Nil = nil, snapshot_topic : String = "") : Sourced::EventList
        after_seq ||= Sourced::Event::ZERO_SEQ
        @registry.from_rs(@db.query(READ_STREAM_SQL, stream_id, after_seq, snapshot_topic))
      end

      def append_to_stream(stream_id : String, events : Sourced::EventList) : Bool
        @logger.debug { "Appending #{events.size} events to stream '#{stream_id}'" }
        @db.transaction do |tx|
          conn = tx.connection
          events.each do |evt|
            conn.exec(
              INSERT_EVENT_SQL,
              stream_id,
              evt.topic,
              evt.seq,
              evt.timestamp,
              evt.payload_json
            )
          end
        end
        true
      rescue err : PQ::PQError
        if err.message =~ PG_EVENT_SEQ_INDEX_EXP
          raise Sourced::Errors::ConcurrencyError.new(err.message)
        else
          raise err
        end
      end
    end
  end
end
