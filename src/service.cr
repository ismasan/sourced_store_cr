require "twirp"
require "db"
require "pg"

require "./consumer_groups"
require "./consumer_groups/pg_store"
require "./twirp_transport/twirp.twirp.cr"
require "./twirp_transport/twirp.pb.cr"

# TODO:
# global fiber listening to PG Notify events
# auto-rebalance consumers after liveness interval
# Advisory lock around consumer, so no duped consumers can consume or ACK at the same time
module SourcedStore
  alias EventList = Array(TwirpTransport::Event)

  struct EventRecord
    include DB::Serializable
    getter id : UUID
    getter topic : String
    getter stream_id : String
    getter global_seq : Int64
    getter seq : Int32
    getter created_at : Time
    getter metadata : JSON::Any | Nil
    getter payload : JSON::Any | Nil

    def metadata_bytes
      metadata.is_a?(Nil) ? nil : metadata.to_json.to_slice
    end

    def payload_bytes : Bytes | Nil
      payload.is_a?(Nil) ? nil : payload.to_json.to_slice
    end
  end

  class Service < SourcedStore::TwirpTransport::EventStore
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

    INSERT_EVENT_SQL = %(insert into event_store.events
            (id, topic, stream_id, seq, created_at, metadata, payload)
            values ($1::uuid, $2, $3, $4, $5::timestamp, $6, $7)
    )

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
      # ToDO: here the event should include the hash_64(stream_id)
      # so that this consumer can ignore the trigger and keep waiting for another one
      # relevant to this consumer
      @pollers = Hash(String, Channel(Bool)).new
      @listen_conn = PG.connect_listen(@db_url, channels: [NOTIFY_CHANNEL], blocking: false) do |n|
        @logger.debug { "PONG #{n.inspect}" }
        @pollers.each_value { |ch| ch.send(true) }
      end
    end

    def info
      %(db: #{@db_url} liveness timeout: #{@liveness_timeout})
    end

    def append_to_stream!(stream_id : String, events : EventList) : TwirpTransport::AppendToStreamResponse
      result = append_to_stream(stream_id, events)
      raise "Could not append: #{result.error.inspect}" unless result.successful

      result
    end

    def append_to_stream(stream_id : String, events : EventList) : TwirpTransport::AppendToStreamResponse
      append_to_stream(TwirpTransport::AppendToStreamRequest.new(
        stream_id: stream_id,
        events: events
      ))
    end

    struct Category
      include DB::Serializable
      getter id : UUID
      getter name : String
    end

    def append_to_stream(req : TwirpTransport::AppendToStreamRequest) : TwirpTransport::AppendToStreamResponse
      events = req.events.as(EventList)
      if !events.any?
        return TwirpTransport::AppendToStreamResponse.new(successful: true)
      end

      @logger.debug { "Appending #{events.size} events to stream '#{req.stream_id}'" }
      @db.transaction do |tx|
        conn = tx.connection
        events.each do |evt|
          conn.exec(
            INSERT_EVENT_SQL,
            evt.id,
            evt.topic,
            evt.stream_id,
            evt.seq,
            protobuf_timestamp_to_time(evt.created_at),
            evt.metadata,
            evt.payload
          )
        end

        #++++++++++++++++++++++++
        cats_to_events = events.each.with_object(Hash(String, Array(String)).new) do |evt, ret|
          evt.topic.as(String).split(".").each do |cat|
            ret[cat] ||= Array(String).new
            ret[cat] << evt.id.as(String)
          end
        end
        all_cats = cats_to_events.keys

        # Create categories
        categories_sql = String.build do |str|
          str << "WITH cats AS(INSERT INTO event_store.categories (name) VALUES "
          str << (1..all_cats.size).map { |pos| "($#{pos})" }.join(", ")
          str << " ON CONFLICT(name) DO NOTHING RETURNING id, name"
          str << ") SELECT * FROM cats UNION SELECT id, name FROM event_store.categories WHERE name IN ("
          str << (1..all_cats.size).map { |pos| "$#{pos}" }.join(", ")
          str << ");"
        end

        category_events = Array(Array(String | UUID)).new

        conn.query(categories_sql, args: all_cats) do |rs|
          Category.from_rs(rs).each.with_index do |cat, idx|
            cats_to_events[cat.name].each do |event_id|
              category_events << [cat.id.as(UUID), event_id.as(String)]
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
        p category_event_sql
        p conn.exec(category_event_sql, args: category_events.flatten)

        #++++++++++++++++++++++++

        # ToDO: include hash_64(stream_id) in notification payload
        # so that listening consumers can ignore notifications that don't concern them
        conn.exec("SELECT pg_notify($1, $2)", NOTIFY_CHANNEL, events.first.id)
      end

      TwirpTransport::AppendToStreamResponse.new(successful: true)
    rescue err
      @logger.error err.inspect
      err_code = case err.message
                 when PG_EVENT_SEQ_INDEX_EXP
                   ErrorCodes::CONCURRENT_WRITE_LOCK_ERROR
                 else
                   "error"
                 end
      TwirpTransport::AppendToStreamResponse.new(
        successful: false,
        error: TwirpTransport::Error.new(
          code: err_code,
          message: err.message
        )
      )
    end

    def read_stream(req : TwirpTransport::ReadStreamRequest) : TwirpTransport::ReadStreamResponse
      events = read_stream(req.stream_id.as(String), req.upto_seq)

      TwirpTransport::ReadStreamResponse.new(events: events)
    end

    def read_stream(stream_id : String, upto_seq : Int32 | Nil = nil) : EventList
      sql = [SELECT_EVENTS_SQL, READ_STREAM_WHERE_SQL] of String

      if upto_seq
        sql << READ_STREAM_AND_UPTO_SQL
        sql << READ_STREAM_ORDER_SQL
        hydrate_events(@db.query(sql.join(" "), stream_id, upto_seq))
      else
        sql << READ_STREAM_ORDER_SQL
        hydrate_events(@db.query(sql.join(" "), stream_id))
      end
    end

    def read_category(category : String, consumer_group : String, consumer_id : String, last_seq : Int64 | Nil = nil) : EventList
      resp = read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: category,
        consumer_group: consumer_group,
        consumer_id: consumer_id,
        wait_timeout: 0,
        last_seq: last_seq
      ))

      resp.events.as(EventList)
    end

    def read_category(req : SourcedStore::TwirpTransport::ReadCategoryRequest) : SourcedStore::TwirpTransport::ReadCategoryResponse
      category = req.category.as(String)
      consumer_group : String = req.consumer_group || "global-group"
      consumer_id : String = req.consumer_id || "global-consumer"
      batch_size : Int32 = req.batch_size || 50
      wait_timeout : Time::Span = req.wait_timeout ? req.wait_timeout.as(Int32).milliseconds : DEFAULT_WAIT_TIMEOUT
      # Checkin a consumer, making sure to debounce its liveness time period for the duration of the wait timeout.
      consumer = @consumer_groups.checkin(consumer_group, consumer_id, wait_timeout, req.last_seq)

      events = read_category_with_consumer(category, consumer, batch_size)
      if !events.any? && !wait_timeout.zero? # blocking poll
        @logger.debug { "no events for consumer #{consumer.info}. Blocking." }
        chan = Channel(Bool).new
        poller_key = [category, consumer.key].join(":")
        @pollers[poller_key] = chan
        timeout = spawn do
          sleep wait_timeout
          chan.send(false)
        end

        if chan.receive
          consumer = @consumer_groups.checkin(consumer_group, consumer_id)
          events = read_category_with_consumer(category, consumer, batch_size)
        end
        @pollers.delete poller_key
      end

      @logger.debug { "finished #{category} #{consumer.info} got #{events.size} events" }
      TwirpTransport::ReadCategoryResponse.new(events: events)
    end

    def ack_consumer(req : TwirpTransport::AckConsumerRequest) : TwirpTransport::AckConsumerResponse
      if !req.last_seq.is_a?(Int64)
        @logger.debug { "ACK last_seq is #{req.last_seq}. Noop" }
        return TwirpTransport::AckConsumerResponse.new(
          successful: false
        )
      end

      success = ack_consumer(
        consumer_group: req.consumer_group.as(String),
        consumer_id: req.consumer_id.as(String),
        last_seq: req.last_seq.as(Int64)
      )
      TwirpTransport::AckConsumerResponse.new(
        successful: success
      )
    end

    def ack_consumer(consumer_group : String, consumer_id : String, last_seq : Int64) : Bool
      @logger.debug { "ACK #{consumer_group} #{consumer_id} at #{last_seq}" }
      @consumer_groups.ack(consumer_group, consumer_id, last_seq)

      true
    end

    def ack_consumer(consumer : ConsumerGroups::Consumer, last_seq : Int64) : Bool
      ack_consumer(consumer_group: consumer.group_name, consumer_id: consumer.id, last_seq: last_seq)
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

    private def time_to_protobuf_timestamp(time : Time)
      Google::Protobuf::Timestamp
      span = time - Time::UNIX_EPOCH
      Google::Protobuf::Timestamp.new(
        seconds: span.total_seconds.to_i,
        nanos: span.nanoseconds
      )
    end

    private def protobuf_timestamp_to_time(pbtime : Google::Protobuf::Timestamp | Nil) : Time
      pbtime = pbtime.as(Google::Protobuf::Timestamp)
      span = Time::Span.new(
        seconds: pbtime.seconds.as(Int64),
        nanoseconds: pbtime.nanos.as(Int32)
      )
      Time::UNIX_EPOCH + span
    end

    private def hydrate_events(rs : ::DB::ResultSet) : EventList
      EventRecord.from_rs(rs).map do |rec|
        TwirpTransport::Event.new(
          id: rec.id.to_s,
          topic: rec.topic,
          stream_id: rec.stream_id,
          global_seq: rec.global_seq,
          seq: rec.seq,
          created_at: time_to_protobuf_timestamp(rec.created_at),
          metadata: rec.metadata_bytes,
          payload: rec.payload_bytes
        )
      end
    end

    private def category_name(topic : String) : String
      topic.split(".").first
    end

    private def read_category_with_consumer(
      category : String,
      consumer : ConsumerGroups::Consumer,
      batch_size : Int32
    ) : EventList
      query = @db.query(
        READ_CATEGORY_SQL,
        category,
        consumer.group_size,
        consumer.position,
        consumer.last_seq,
        batch_size
      )
      hydrate_events(query)
    end
  end
end
