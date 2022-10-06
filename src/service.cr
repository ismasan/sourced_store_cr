require "twirp"
require "db"
require "pg"

require "./consumer_groups"
require "./twirp_transport/twirp.twirp.cr"
require "./twirp_transport/twirp.pb.cr"

module SourcedStore
  alias EventList = Array(TwirpTransport::Event)

  struct EventRecord
    include DB::Serializable
    getter id : UUID
    getter topic : String
    getter stream_id : String
    getter originator_id : UUID | Nil
    getter global_seq : Int64
    getter seq : Int32
    getter created_at : Time
    getter payload : JSON::Any | Nil

    def payload_bytes : Bytes | Nil
      payload.is_a?(Nil) ? nil : payload.to_json.to_slice
    end
  end

  class Service < SourcedStore::TwirpTransport::EventStore
    module ErrorCodes
      CONCURRENT_WRITE_LOCK_ERROR = "concurrent_write_lock_error"
    end

    DEFAULT_WAIT_TIMEOUT = 10000 # 10 seconds
    PG_EVENT_SEQ_INDEX_EXP = /unique_index_on_event_seqs/

    SELECT_EVENTS_SQL = %(select
            id,
            topic,
            stream_id,
            originator_id,
            global_seq,
            seq,
            created_at,
            payload
            from event_store.events)

    READ_STREAM_WHERE_SQL    = %(WHERE stream_id = $1)
    READ_STREAM_AND_UPTO_SQL = %(AND seq <= $2)
    READ_STREAM_ORDER_SQL    = %(ORDER BY seq ASC)

    INSERT_EVENT_SQL = %(insert into event_store.events
            (id, topic, stream_id, originator_id, seq, created_at, payload)
            values ($1::uuid, $2, $3, $4, $5, $6::timestamp, $7)
    )

    # $1 category
    # $2 group size
    # $3 consumer number
    # $4 after_global_seq
    # $5 batch_size
    READ_CATEGORY_WHERE_SQL = %(
      WHERE event_store.event_category(topic) = $1
      AND MOD(event_store.hash_64(stream_id::varchar), $2) = $3
      AND global_seq > $4
      ORDER BY global_seq ASC
      LIMIT $5
    )

    @db : DB::Database
    @consumer_groups : ConsumerGroups

    def initialize(logger : Logger, db_url : String, liveness_timeout : Int32 = ConsumerGroups::DEFAULT_LIVENESS_TIMEOUT)
      @logger = logger
      @db_url = db_url
      @db = DB.open(@db_url)
      @consumer_groups = ConsumerGroups.new(
        logger: @logger,
        liveness_timeout: liveness_timeout
      )
    end

    def info
      %(db: #{@db_url} liveness timeout: #{@consumer_groups.liveness_timeout})
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

    def append_to_stream(req : TwirpTransport::AppendToStreamRequest) : TwirpTransport::AppendToStreamResponse
      events = req.events.as(EventList)
      if !events.any?
        return TwirpTransport::AppendToStreamResponse.new(successful: true)
      end

      @logger.info "Appending #{events.size} events to stream '#{req.stream_id}'"
      @db.transaction do |tx|
        conn = tx.connection
        events.each do |evt|
          conn.exec(
            INSERT_EVENT_SQL,
            evt.id,
            evt.topic,
            evt.stream_id,
            evt.originator_id,
            evt.seq,
            protobuf_timestamp_to_time(evt.created_at),
            evt.payload
          )
        end

        # ToDO: include hash_64(stream_id) in notification payload
        # so that listening consumers can ignore notifications that don't concern them
        conn.exec("SELECT pg_notify($1, $2)", category_name(events.first.topic.as(String)), events.first.id)
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
      sql = [SELECT_EVENTS_SQL, READ_STREAM_WHERE_SQL] of String

      events = if req.upto_seq
                 sql << READ_STREAM_AND_UPTO_SQL
                 sql << READ_STREAM_ORDER_SQL
                 hydrate_events(@db.query(sql.join(" "), req.stream_id, req.upto_seq))
               else
                 sql << READ_STREAM_ORDER_SQL
                 hydrate_events(@db.query(sql.join(" "), req.stream_id))
               end

      TwirpTransport::ReadStreamResponse.new(events: events)
    end

    # $1 category
    # $2 group size
    # $3 consumer number
    # $4 after_global_seq
    # $5 batch_size
    def read_category(category : String, consumer_group : String, consumer_id : String) : EventList
      resp = read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: category,
        consumer_group: consumer_group,
        consumer_id: consumer_id,
        wait_timeout: 0
      ))

      resp.events.as(EventList)
    end

    def read_category(req : SourcedStore::TwirpTransport::ReadCategoryRequest) : SourcedStore::TwirpTransport::ReadCategoryResponse
      category = req.category.as(String)
      consumer_group : String = req.consumer_group || "global-group"
      consumer_id : String = req.consumer_id || "global-consumer"
      batch_size : Int32 = req.batch_size || 50
      wait_timeout : Time::Span = (req.wait_timeout || DEFAULT_WAIT_TIMEOUT).milliseconds

      sql = [SELECT_EVENTS_SQL, READ_CATEGORY_WHERE_SQL] of String

      consumer = @consumer_groups.checkin(consumer_group, consumer_id)
      events = read_category_with_consumer(category, consumer, batch_size)
      if !events.any? && !wait_timeout.zero? # blocking poll
        chan = Channel(Nil).new
        timeout = spawn do
          sleep wait_timeout
          chan.send(nil)
        end

        # ToDO: here the event should include the hash_64(stream_id)
        # so that this consumer can ignore the trigger and keep waiting for another one
        # relevant to this consumer
        listen_conn = PG.connect_listen(@db_url, channels: [category], blocking: false) do |n|
          @logger.info "#{consumer.info}: PONG #{n.inspect}"
          chan.send nil
        end
        chan.receive
        listen_conn.close
        events = read_category_with_consumer(category, consumer, batch_size)
      end

      if events.any?
        @consumer_groups.notify_consumer(consumer, events.last.global_seq.as(Int64))
      end
      @logger.info "finished #{category} #{consumer.info} got #{events.size} events"
      TwirpTransport::ReadCategoryResponse.new(events: events)
    ensure
      @consumer_groups.checkout(consumer)
    end

    def stop
      @logger.info "CLOSING DB"
      @db.close
    end

    def reset!
      return unless ENV["ENV"] == "test"

      @logger.info "Resetting DB. Careful!"
      @consumer_groups = ConsumerGroups.new(logger: @logger)
      @db.exec("TRUNCATE event_store.events RESTART IDENTITY")
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
        originator_id : String | Nil = rec.originator_id.to_s
        originator_id = nil if originator_id == ""

        TwirpTransport::Event.new(
          id: rec.id.to_s,
          topic: rec.topic,
          stream_id: rec.stream_id,
          originator_id: originator_id,
          global_seq: rec.global_seq,
          seq: rec.seq,
          created_at: time_to_protobuf_timestamp(rec.created_at),
          payload: rec.payload_bytes
        )
      end
    end

    private def category_name(topic : String) : String
      topic.split(".").first
    end

    private def read_category_with_consumer(
      category : String,
      consumer : SourcedStore::ConsumerGroups::Consumer,
      batch_size : Int32
    ) : EventList
      sql = [SELECT_EVENTS_SQL, READ_CATEGORY_WHERE_SQL] of String
      query = @db.query(
        sql.join(" "),
        category,
        consumer.group_size,
        consumer.number,
        consumer.last_global_seq,
        batch_size
      )
      hydrate_events(query)
    end
  end
end
