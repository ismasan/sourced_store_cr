require "logger"
require "uuid"
require "./spec_helper"
require "../src/cli/setup.cr"
require "../src/service"

describe SourcedStore::Service do
  test_db_url = "postgres://localhost/sourced_store_test"
  service = uninitialized SourcedStore::Service
  logger = Logger.new(STDOUT, level: Logger::INFO)
  stream_id1 = "stream10" # partitions to consumer 0 of 2
  stream_id2 = "stream20" # partitions to consumer 1 of 2
  request_events = [
    build_event(
      topic: "orders.start",
      stream_id: stream_id1,
      seq: 1,
      created_at: 1664718079,
      metadata: nil,
      payload: nil
    ),
    build_event(
      topic: "orders.place",
      stream_id: stream_id1,
      seq: 2,
      created_at: 1664718080,
      metadata: nil,
      payload: nil
    ),
  ] of SourcedStore::TwirpTransport::Event

  append_req = SourcedStore::TwirpTransport::AppendToStreamRequest.new(
    stream_id: stream_id1,
    events: request_events
  )

  before_all do
    CLI::Setup.run(["--database=#{test_db_url}"])
    service = SourcedStore::Service.new(
      logger: logger,
      db_url: test_db_url,
      liveness_timeout: 10.milliseconds
    )
    service.reset!
  end

  after_all do
    service.stop
  end

  describe "#append_to_stream, #read_stream" do
    it "appends and reads events" do
      resp = service.append_to_stream(req: append_req)
      resp.should be_a(SourcedStore::TwirpTransport::AppendToStreamResponse)
      resp.successful.should eq(true)
      resp.error.should eq(nil)

      read_resp = service.read_stream(
        SourcedStore::TwirpTransport::ReadStreamRequest.new(
          stream_id: stream_id1
        )
      )
      read_resp.should be_a(SourcedStore::TwirpTransport::ReadStreamResponse)
      events = read_resp.events.as(SourcedStore::EventList)
      sent_events = append_req.events.as(SourcedStore::EventList)
      events.size.should eq(2)
      assert_same_event(events.first, sent_events.first)
      (events[0].global_seq.as(Int64) < events[1].global_seq.as(Int64)).should eq(true)
    end

    it "fails if expected_seq doesn't match" do
      service.append_to_stream(req: append_req)
      new_req = SourcedStore::TwirpTransport::AppendToStreamRequest.new(
        stream_id: stream_id1,
        expected_seq: 2,
        events: [
          build_event(
            topic: "orders.close",
            stream_id: stream_id1,
            seq: 2,
            created_at: 1664718080,
            metadata: nil,
            payload: nil
          ),
        ] of SourcedStore::TwirpTransport::Event
      )
      resp = service.append_to_stream(req: new_req)
      resp.successful.should eq(false)
      error = resp.error.as(SourcedStore::TwirpTransport::Error)
      error.code.should eq("concurrent_write_lock_error")
      error.message.should eq("duplicate key value violates unique constraint \"unique_index_on_event_seqs\"")
    end

    it "reads :upto_seq" do
      service.append_to_stream(req: append_req)
      read_resp = service.read_stream(
        SourcedStore::TwirpTransport::ReadStreamRequest.new(
          stream_id: stream_id1,
          upto_seq: 1
        )
      )
      read_resp.should be_a(SourcedStore::TwirpTransport::ReadStreamResponse)
      events = read_resp.events.as(SourcedStore::EventList)
      sent_events = append_req.events.as(SourcedStore::EventList)
      events.size.should eq(1)
      assert_same_event(events.first, sent_events.first)
    end
  end

  describe "#read_category" do
    it "reads category stream" do
      service.append_to_stream(req: append_req)

      order_event3 = build_event(
        topic: "orders.commands.start",
        stream_id: stream_id2,
        seq: 1,
        created_at: 1664718011,
        metadata: nil,
        payload: nil
      )

      service.append_to_stream!(stream_id2, [order_event3])

      service.append_to_stream!(stream_id2, [
        build_event(
          topic: "accounts.open",
          stream_id: "account1",
          seq: 1,
          created_at: 1664718011,
          metadata: nil,
          payload: nil
        ),
      ])

      resp = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders"
      ))
      events = resp.events.as(SourcedStore::EventList)
      sent_events = append_req.events.as(SourcedStore::EventList)
      events.size.should eq(3)
      assert_same_event(events[0], sent_events[0])
      assert_same_event(events[1], sent_events[1])
      assert_same_event(events[2], order_event3)

      # It ANDs categories together
      resp = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders.commands"
      ))
      events = resp.events.as(SourcedStore::EventList)
      events.size.should eq(1)
      events.map(&.stream_id).should eq([stream_id2])
    end

    it "optionally auto-acks consumer to provided last_seq" do
      service.reset!
      service.append_to_stream!(stream_id1, append_req.events.as(SourcedStore::EventList))
      events = service.read_stream(
        SourcedStore::TwirpTransport::ReadStreamRequest.new(
          stream_id: stream_id1
        )
      ).events.as(SourcedStore::EventList)

      # one request to checkin consumer
      service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "c1"
      ))

      # Now another one, auto-acking at an event's last global_seq
      c1_events = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "c1",
        last_seq: events.first.global_seq
      )).events.as(SourcedStore::EventList)
      c1_events.size.should eq(1)
    end

    it "partitions stream by consumers" do
      service.reset!
      service.append_to_stream!(stream_id1, append_req.events.as(SourcedStore::EventList))

      # Group starts with 1 consumer/partition
      # that returns all events so far
      c1_events = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "c1",
        wait_timeout: 0
      )).events.as(SourcedStore::EventList)
      c1_events.size.should eq(2)
      service.ack_consumer("sale-report", "c1", c1_events.last.global_seq.as(Int64))

      # Group is now up to date, so a new consumer has no new events to fetch
      c2_events = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "c2",
        wait_timeout: 0
      )).events.as(SourcedStore::EventList)

      c2_events.size.should eq(0)

      # Group now has 2 partitions so, after adding new events, c1 and c2 get a subset of events
      service.append_to_stream!(stream_id1, [
        build_event(
          topic: "orders.start",
          stream_id: stream_id1,
          seq: 3,
          created_at: 1664718011,
          metadata: nil,
          payload: nil
        ),
      ])
      service.append_to_stream!(stream_id2, [
        build_event(
          topic: "orders.start",
          stream_id: stream_id2,
          seq: 1,
          created_at: 1664718011,
          metadata: nil,
          payload: nil
        ),
      ])

      c1_events = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "c1"
      )).events.as(SourcedStore::EventList)
      service.ack_consumer("sale-report", "c1", c1_events.last.global_seq.as(Int64))

      c1_events.size.should eq(1)
      c1_events.map(&.stream_id).should eq([stream_id1])

      c2_events = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "c2"
      )).events.as(SourcedStore::EventList)
      c2_events.size.should eq(1)
      c2_events.map(&.stream_id).should eq([stream_id2])
    end

    it "blocks and waits for new events if none found yet" do
      service.reset!

      spawn do
        sleep 0.01
        service.append_to_stream(req: append_req)
      end

      consumer_1_resp = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "another-group",
        consumer_id: "timeout-exceeded",
        wait_timeout: 200, # milliseconds
      ))
      events = consumer_1_resp.events.as(SourcedStore::EventList)
      events.size.should eq(2)
    end

    it "returns if timeout exceeded after polling" do
      service.reset!
      consumer_1_resp = service.read_category(SourcedStore::TwirpTransport::ReadCategoryRequest.new(
        category: "orders",
        consumer_group: "sale-report",
        consumer_id: "timeout-exceeded",
        wait_timeout: 5, # milliseconds
      ))

      events = consumer_1_resp.events.as(SourcedStore::EventList)
      events.size.should eq(0)
    end

    it "un-registers stale consumers, and re-balances group" do
    end
  end

  it "supports event metadata" do
  end
end

private def build_event(
  topic : String | Nil,
  stream_id : String | Nil,
  seq : Int32 | Nil,
  created_at : Int32 | Nil,
  metadata : Slice(UInt8) | Nil,
  payload : Slice(UInt8) | Nil
)
  cat : Google::Protobuf::Timestamp | Nil = nil
  cat = Google::Protobuf::Timestamp.new(seconds: created_at, nanos: 0) if created_at

  SourcedStore::TwirpTransport::Event.new(
    id: UUID.random.to_s,
    topic: topic,
    stream_id: stream_id,
    seq: seq,
    created_at: cat,
    metadata: metadata,
    payload: payload
  )
end

private def assert_same_event(e1, e2)
  e1.id.should eq(e2.id)
  e1.topic.should eq(e2.topic)
  e1.stream_id.should eq(e2.stream_id)
  e1.seq.should eq(e2.seq)
  e1.created_at.should eq(e2.created_at)
  e1.metadata.should eq(e2.metadata)
  e1.payload.should eq(e2.payload)
end
