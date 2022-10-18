# frozen_string_literal: true

require 'json'
require 'google/protobuf/well_known_types'
require 'sourced_client/protos/twirp_twirp'

module SourcedClient
  class TransportError < StandardError
    def initialize(resp)
      super resp.error.inspect
    end
  end

  class Client
    def initialize(endpoint: 'http://127.0.0.1:8080/twirp')
      @client = SourcedClient::TwirpTransport::EventStoreClient.new(endpoint)
      @shutting_down = false
    end

    def read_stream(stream_id, upto_seq: nil)
      resp = client.read_stream(stream_id: stream_id, upto_seq: upto_seq)
      # TODO should this client return Result objects
      raise TransportError.new(resp) if resp.error

      deserialize_events(resp.data.events)
    end

    def append_to_stream(stream_id, events, expected_seq: nil)
      events = serialize_events(events)
      resp = client.append_to_stream(stream_id: stream_id, expected_seq: expected_seq, events: events)
      raise TransportError.new(resp) if resp.error
      # TODO should this client return Result objects
      raise TransportError.new(resp.data) unless resp.data.successful

      resp.data.successful
    end

    def shutdown
      @shutting_down = true
    end

    def read_category(category, after_global_seq: 0, consumer_group: nil, consumer_id: nil)
      resp = client.read_category(
        category: category,
        after_global_seq: after_global_seq,
        consumer_group: consumer_group,
        consumer_id: consumer_id
      )

      deserialize_events(resp.data.events)
    end

    def stream_category(category, after_global_seq: 0, consumer_group: nil, consumer_id: nil)
      last_read_seq = 0

      Enumerator.new do |yielder|
        while !@shutting_down do
          begin
            events = read_category(
              category,
              after_global_seq: after_global_seq,
              consumer_group: consumer_group,
              consumer_id: consumer_id
            )

            events.each do |e|
              yielder << e
              last_read_seq = e[:global_seq]
            end

            if events.any?
              client.ack_consumer(
                consumer_group: consumer_group,
                consumer_id: consumer_id,
                last_seq: last_read_seq
              )
            end
          rescue Faraday::ConnectionFailed
            puts 'Disconnected. Retrying in 5...'
            sleep 5
          rescue StandardError => ex
            puts "Error #{ex}. ACKing and bailing."
            client.ack_consumer(
              consumer_group: consumer_group,
              consumer_id: consumer_id,
              last_seq: last_read_seq
            )

            raise
          end
        end
      end
    end

    def ack_consumer(consumer_group:, consumer_id:, last_seq:)
      puts "ACK #{consumer_group} #{consumer_id}: #{last_seq}"
      client.ack_consumer(
        consumer_group: consumer_group,
        consumer_id: consumer_id,
        last_seq: last_seq
      )
    end

    private

    attr_reader :client

    def serialize_events(events)
      events.map(&:to_h).map do |evt|
        evt[:payload] = JSON.dump(evt[:payload]) if evt[:payload]
        evt[:created_at] = Google::Protobuf::Timestamp.from_time(evt[:created_at]) if evt[:created_at]
        evt
      end
    end

    def deserialize_events(events)
      events.map do |evt|
        e = evt.to_h
        if e[:payload]
          e[:payload] = JSON.parse(e[:payload], symbolize_names: true)
        end
        e[:created_at] = evt.created_at.to_time
        e
      end
    end
  end
end
