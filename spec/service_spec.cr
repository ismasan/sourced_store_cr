require "logger"
require "uuid"
require "./spec_helper"
require "../src/cli/setup.cr"
require "../src/service"

describe SourcedStore::Service do
  test_db_url = "postgres://localhost/sourced_store_test"
  service = uninitialized SourcedStore::Service
  logger = Logger.new(STDOUT, level: Logger::INFO)

  before_all do
    CLI::Setup.run(["--database=#{test_db_url}"])
    service = SourcedStore::Service.new(logger: logger, db_url: test_db_url)
  end

  after_all do
    service.stop
  end

  before_each do
    service.reset!
  end

  describe "#append_to_stream, #read_stream" do
    stream_id = "test-stream-1"
    request_events = [
      build_event(
        id: UUID.random.to_s,
        topic: "orders.start",
        stream_id: stream_id,
        originator_id: nil,
        seq: 1,
        created_at: Google::Protobuf::Timestamp.new(seconds: 1664718079, nanos: 0),
        payload: nil
      ),
      build_event(
        id: UUID.random.to_s,
        topic: "orders.place",
        stream_id: stream_id,
        originator_id: nil,
        seq: 2,
        created_at: Google::Protobuf::Timestamp.new(seconds: 1664718080, nanos: 0),
        payload: nil
      ),
    ] of SourcedStore::TwirpTransport::Event

    req = SourcedStore::TwirpTransport::AppendToStreamRequest.new(
      stream_id: stream_id,
      expected_seq: 2,
      events: request_events
    )

    it "appends and reads events" do
      resp = service.append_to_stream(req: req)
      resp.should be_a(SourcedStore::TwirpTransport::AppendToStreamResponse)
      resp.successful.should eq(true)
      resp.error.should eq(nil)

      read_resp = service.read_stream(
        SourcedStore::TwirpTransport::ReadStreamRequest.new(
          stream_id: stream_id
        )
      )
      read_resp.should be_a(SourcedStore::TwirpTransport::ReadStreamResponse)
      events = read_resp.events.as(Array(SourcedStore::TwirpTransport::Event))
      sent_events = req.events.as(Array(SourcedStore::TwirpTransport::Event))
      events.size.should eq(2)
      events.first.should eq(sent_events.first)
    end

    it "fails if expected_seq doesn't match" do
      service.append_to_stream(req: req)
      new_req = SourcedStore::TwirpTransport::AppendToStreamRequest.new(
        stream_id: stream_id,
        expected_seq: 2,
        events: [
          build_event(
            id: UUID.random.to_s,
            topic: "orders.close",
            stream_id: stream_id,
            originator_id: nil,
            seq: 2,
            created_at: Google::Protobuf::Timestamp.new(seconds: 1664718080, nanos: 0),
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
      service.append_to_stream(req: req)
      read_resp = service.read_stream(
        SourcedStore::TwirpTransport::ReadStreamRequest.new(
          stream_id: stream_id,
          upto_seq: 1
        )
      )
      read_resp.should be_a(SourcedStore::TwirpTransport::ReadStreamResponse)
      events = read_resp.events.as(Array(SourcedStore::TwirpTransport::Event))
      sent_events = req.events.as(Array(SourcedStore::TwirpTransport::Event))
      events.size.should eq(1)
      events.first.should eq(sent_events.first)
    end
  end
end

private def build_event(
  id : String | Nil,
  topic : String | Nil,
  stream_id : String | Nil,
  originator_id : String | Nil,
  seq : Int32 | Nil,
  created_at : Google::Protobuf::Timestamp | Nil,
  payload : Slice(UInt8) | Nil
)
  SourcedStore::TwirpTransport::Event.new(
    id: id,
    topic: topic,
    stream_id: stream_id,
    originator_id: originator_id,
    seq: seq,
    created_at: created_at,
    payload: payload
  )
end
