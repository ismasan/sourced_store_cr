require "db"
require "pg"

require "./pg_backend/setup"
require "./backend_interface"
require "./structs"
require "./consumer_groups"
require "./consumer_groups/pg_store"

# TODO:
# Advisory lock around consumer, so no duped consumers can consume or ACK at the same time
module SourcedStore
  class PGBackend
    include BackendInterface

    module ErrorCodes
      CONCURRENT_WRITE_LOCK_ERROR = "concurrent_write_lock_error"
    end

    DEFAULT_WAIT_TIMEOUT             = 10.seconds # 10 seconds
    DEFAULT_COMPACT_INTERVAL         = 30.minutes
    DEFAULT_LIVENESS_TIMEOUT         = 5.seconds
    DEFAULT_CONSUMERS_SNAPSHOT_EVERY = 100 # snapshot consumer groups every 100 events
    PG_EVENT_SEQ_INDEX_EXP           = /unique_index_on_event_seqs/

    SELECT_EVENTS_SQL = %(select
            id,
            topic,
            stream_id,
            global_seq,
            seq,
            created_at,
            metadata,
            payload
            from event_store.events)

    READ_STREAM_WHERE_SQL    = %(WHERE stream_id = $1)
    READ_STREAM_AND_UPTO_SQL = %(AND seq <= $2)
    READ_STREAM_ORDER_SQL    = %(ORDER BY seq ASC)

    # $1 category
    # $2 consumer_group
    # $3 consumer_id
    # $4 batch_size
    READ_CATEGORY_SQL = %(
      SELECT * FROM event_store.read_category($1::varchar, $2::integer, $3::integer, $4::bigint, $5::integer)
    )

    ACK_CONSUMER_SQL = %(
      SELECT * FROM event_store.checkout_consumer($1::varchar, $2::varchar, $3::bigint)
    )

    NOTIFY_CHANNEL = "new-events"

    class ConsumerNotifier
      getter chan : Channel(Bool)

      def initialize(@group_size : Int32, @consumer_position : Int32, @wait_timeout : Time::Span)
        @chan = Channel(Bool).new

        spawn do
          sleep @wait_timeout
          @chan.send(false)
        end
      end

      def call(stream_id_hash : Int128)
        return unless stream_id_hash % @group_size == @consumer_position

        @chan.send(true)
      end
    end

    @db : DB::Database
    @listen_conn : PG::ListenConnection

    def initialize(
      @logger : Logger,
      @db_url : String,
      @liveness_timeout : Time::Span = DEFAULT_LIVENESS_TIMEOUT,
      compact_every : Time::Span = DEFAULT_COMPACT_INTERVAL,
      snapshot_every : Int32 = DEFAULT_CONSUMERS_SNAPSHOT_EVERY,
      keep_snapshots : Int32 = 1
    )
      @db = DB.open(@db_url)
      @consumer_groups = SourcedStore::ConsumerGroups.new(
        # store: Sourced::MemStore.new,
        store: SourcedStore::ConsumerGroups::PGStore.new(@db, @logger),
        liveness_span: @liveness_timeout,
        logger: @logger,
        snapshot_every: snapshot_every, # snapshot consumer group every X events
        compact_every: compact_every,   # compact consumer group streams every Z interval,
        keep_snapshots: keep_snapshots  # keep this many snapshots per stream when compacting.
      )

      @pollers = Hash(String, ConsumerNotifier).new
      @listen_conn = PG.connect_listen(@db_url, channels: [NOTIFY_CHANNEL], blocking: false) do |n|
        stream_hash = n.payload.to_i128
        @logger.debug { "PONG #{stream_hash}" }
        @pollers.each_value { |ch| ch.call(stream_hash) }
      end
    end

    def info : String
      %(db: #{@db_url} liveness timeout: #{@liveness_timeout})
    end

    def append_to_stream(stream_id : String, events : Array(EventRecord)) : ResultWithError
      if !events.any?
        return ResultWithError.new(successful: true, error: nil)
      end

      @logger.debug { "Appending #{events.size} events to stream '#{stream_id}'" }
      @db.transaction do |tx|
        conn = tx.connection
        db_insert_events(conn, events)
        db_categorize_events(conn, events)

        # ToDO: include hash_64(stream_id) in notification payload
        # so that listening consumers can ignore notifications that don't concern them
        conn.exec("SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, stream_id_hash_for(stream_id))
      end

      ResultWithError.new(successful: true, error: nil)
    rescue err
      @logger.error err.inspect
      err_code = case err.message
                 when PG_EVENT_SEQ_INDEX_EXP
                   ErrorCodes::CONCURRENT_WRITE_LOCK_ERROR
                 else
                   "error"
                 end
      ResultWithError.new(successful: false, error: Error.new(code: err_code, message: err.message))
    end

    def read_stream(stream_id : String, upto_seq : Int32 | Nil = nil) : Array(EventRecord)
      sql = [SELECT_EVENTS_SQL, READ_STREAM_WHERE_SQL] of String

      if upto_seq
        sql << READ_STREAM_AND_UPTO_SQL
        sql << READ_STREAM_ORDER_SQL
        EventRecord.from_rs(@db.query(sql.join(" "), stream_id, upto_seq))
      else
        sql << READ_STREAM_ORDER_SQL
        EventRecord.from_rs(@db.query(sql.join(" "), stream_id))
      end
    end

    def read_category(
      category : String,
      consumer_group : String = "global-group",
      consumer_id : String = "global-consumer",
      last_seq : Int64 | Nil = nil,
      batch_size : Int32 = 50,
      wait_timeout : Time::Span | Nil = nil
    ) : Array(EventRecord)
      # Checkin a consumer, making sure to debounce its liveness time period for the duration of the wait timeout.
      wait_timeout ||= DEFAULT_WAIT_TIMEOUT
      consumer = @consumer_groups.checkin(consumer_group, consumer_id, wait_timeout, last_seq)


      events = read_category_with_consumer(category, consumer, batch_size)
      if !events.any? && !wait_timeout.zero? # blocking poll
        @logger.debug { "no events for consumer #{consumer.info}. Blocking." }
        poller_key = [category, consumer.key].join(":")
        @pollers[poller_key] = ConsumerNotifier.new(consumer.group_size, consumer.position, wait_timeout)

        if @pollers[poller_key].chan.receive
          consumer = @consumer_groups.checkin(consumer_group, consumer_id)
          events = read_category_with_consumer(category, consumer, batch_size)
        end
        @pollers.delete poller_key
      end

      @logger.debug { "finished #{category} #{consumer.info} got #{events.size} events" }
      events
    end

    def ack_consumer(consumer_group : String, consumer_id : String, last_seq : Int64?) : Bool
      if !last_seq.is_a?(Int64)
        @logger.debug { "ACK last_seq is #{last_seq}. Noop" }
        return false
      end

      @logger.debug { "ACK #{consumer_group} #{consumer_id} at #{last_seq}" }
      @consumer_groups.ack(consumer_group, consumer_id, last_seq)

      true
    end

    def setup
      Setup.run(@db, @logger)
    end

    def stop
      @logger.debug { "CLOSING DB" }
      @listen_conn.close
      @db.close
    end

    def reset!
      return unless ENV["ENV"] == "test"

      @logger.info "Resetting DB. Careful!"
      @db.exec("DELETE FROM event_store.categories")
      @db.exec("DELETE FROM event_store.categories_to_events")
      @db.exec("DELETE FROM event_store.events")
      @consumer_groups.reset!
    end

    private def read_category_with_consumer(
      category : String,
      consumer : ConsumerGroups::Consumer,
      batch_size : Int32
    ) : Array(EventRecord)
      query = @db.query(
        READ_CATEGORY_SQL,
        category,
        consumer.group_size,
        consumer.position,
        consumer.last_seq,
        batch_size
      )
      EventRecord.from_rs(query)
    end

    INSERT_EVENTS_START_SQL = %(INSERT INTO event_store.events (id, topic, stream_id, seq, created_at, metadata, payload) VALUES )

    private def stream_id_hash_for(stream_id : String) : Int128
      Digest::MD5.hexdigest(stream_id)[0...16].to_i128(16).abs
    end

    private def db_insert_events(conn, events)
      events_sql = String.build do |str|
        str << INSERT_EVENTS_START_SQL
        count = 0
        str << events.map do |evt|
          String.build do |f|
            count += 1
            f << "($#{count}::uuid, "
            count += 1
            f << "$#{count}, " # topic
            count += 1
            f << "$#{count}, " # stream_id
            count += 1
            f << "$#{count}, " # seq
            count += 1
            f << "$#{count}::timestamp, " # created_at
            count += 1
            f << "$#{count}, " # metadata
            count += 1
            f << "$#{count})" # payload
          end
        end.join(", ")
      end

      event_rows = events.flat_map(&.to_a)
      conn.exec(events_sql, args: event_rows)
    end

    private def db_categorize_events(conn, events)
      cats_to_events = events.each.with_object(Hash(String, Array(UUID)).new) do |evt, ret|
        evt.topic.as(String).split(".").each do |cat|
          ret[cat] ||= Array(UUID).new
          ret[cat] << evt.id
        end
      end
      all_cats = cats_to_events.keys

      # Upsert categories
      categories_sql = String.build do |str|
        str << "WITH cats AS(INSERT INTO event_store.categories (name) VALUES "
        str << (1..all_cats.size).map { |pos| "($#{pos})" }.join(", ")
        str << " ON CONFLICT(name) DO NOTHING RETURNING id, name"
        str << ") SELECT * FROM cats UNION SELECT id, name FROM event_store.categories WHERE name IN ("
        str << (1..all_cats.size).map { |pos| "$#{pos}" }.join(", ")
        str << ");"
      end

      category_events = Array(Array(UUID)).new

      conn.query(categories_sql, args: all_cats) do |rs|
        Category.from_rs(rs).each.with_index do |cat, idx|
          cats_to_events[cat.name].each do |event_id|
            category_events << [cat.id.as(UUID), event_id]
          end
        end
      end

      # Create categories_to_events
      category_event_sql = String.build do |str|
        str << "INSERT INTO event_store.categories_to_events (category_id, event_id) VALUES "
        count = 0
        str << category_events.map do |(cat_id, event_id)|
          String.build do |f|
            count += 1
            f << "($#{count}, "
            count += 1
            f << "$#{count})"
          end
        end.join(", ")
      end
      conn.exec(category_event_sql, args: category_events.flatten)
    end
  end
end
