require "admiral"
require "db"
require "pg"

module CLI
  class Setup < Admiral::Command
    SQL_SCHEMA = <<-SQL
    CREATE SCHEMA IF NOT EXISTS event_store;
    SQL

    SQL_EXTENSIONS = <<-SQL
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    SQL

    SQL_CREATE_EVENTS_TABLE = <<-SQL
    CREATE TABLE IF NOT EXISTS event_store.events (
      id uuid DEFAULT uuid_generate_v4(),
      topic varchar NOT NULL,
      stream_id varchar NOT NULL,
      originator_id uuid,
      global_seq BIGSERIAL NOT NULL,
      seq integer NOT NULL DEFAULT 0,
      created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,
      payload json,
      CONSTRAINT pk_events_global_seq PRIMARY KEY (global_seq)
    );
    SQL

    SQL_CREATE_INTERNAL_EVENTS_TABLE = <<-SQL
    CREATE TABLE IF NOT EXISTS event_store.internal_events (
      id SERIAL PRIMARY KEY,
      topic character varying NOT NULL,
      stream_id character varying NOT NULL,
      seq bigint NOT NULL DEFAULT 0,
      timestamp timestamp without time zone NOT NULL DEFAULT timezone('utc'::text, now()),
      payload json
    );
    SQL

    SQL_FN_HASH64 = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.hash_64(
      value varchar
    )
    RETURNS bigint
    AS $$
    DECLARE
      _hash bigint;
    BEGIN
      SELECT ABS(left('x' || md5(hash_64.value), 17)::bit(64)::bigint) INTO _hash;
      return _hash;
    END;
    $$ LANGUAGE plpgsql
    IMMUTABLE;
    SQL

    SQL_FN_READ_CATEGORY = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.read_category(
      category varchar,
      group_size integer,
      consumer_position integer,
      after_seq bigint,
      batch_size integer
    )
    RETURNS SETOF event_store.events
    AS $$
    DECLARE
    BEGIN
      RETURN QUERY
        SELECT event_store.events.*
        FROM event_store.events
        WHERE event_store.event_category(topic) = read_category.category
              AND MOD(event_store.hash_64(stream_id::varchar), read_category.group_size) = read_category.consumer_position
              AND global_seq > read_category.after_seq
              ORDER BY global_seq ASC
              LIMIT read_category.batch_size;
    END;
    $$ LANGUAGE plpgsql
    VOLATILE;
    SQL

    SQL_FN_READ_INTERNAL_STREAM = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.read_internal_stream(
      stream_id varchar,
      after_seq bigint,
      snapshot_topic varchar
    )
    RETURNS SETOF event_store.internal_events
    AS $$
    DECLARE
    	snapshot record;
    BEGIN
      SELECT MAX(seq) last_seq
      	FROM event_store.internal_events e
      	INTO snapshot
      	WHERE e.stream_id = read_internal_stream.stream_id
      	AND e.topic = read_internal_stream.snapshot_topic;

      IF (read_internal_stream.after_seq = 0 AND snapshot.last_seq IS NOT NULL) THEN
      	RETURN QUERY
          SELECT e.* FROM event_store.internal_events e
      		WHERE e.stream_id = read_internal_stream.stream_id
      		AND e.seq >= snapshot.last_seq
      		ORDER BY e.seq ASC;
      ELSE
      	RETURN QUERY
          SELECT e.* FROM event_store.internal_events e
      		WHERE e.stream_id = read_internal_stream.stream_id
      		AND e.seq > read_internal_stream.after_seq
      		ORDER BY e.seq ASC;
      END IF;
    END;
    $$ LANGUAGE plpgsql
    VOLATILE;
    SQL

    # Crystal's DB#exec can only take single commands :(
    SQL_INDICES = [
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_ids ON event_store.events (id)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_seqs ON event_store.events (stream_id, seq)),
      %(CREATE INDEX IF NOT EXISTS index_on_event_categories ON event_store.events (event_store.event_category(topic))),
      %(CREATE UNIQUE INDEX IF NOT EXISTS internal_events_pkey ON event_store.internal_events(id int4_ops)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_internal_event_seqs ON event_store.internal_events(stream_id text_ops,seq int8_ops)),
      %(CREATE INDEX IF NOT EXISTS index_on_internal_event_topics ON event_store.internal_events(topic)),
    ]

    SQL_FN_EVENT_CATEGORY = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.event_category(topic varchar) RETURNS varchar AS $$
    BEGIN
      RETURN SPLIT_PART(event_category.topic, '.', 1);
    END;
    $$ LANGUAGE plpgsql
    IMMUTABLE;
    SQL

    define_flag database : String, required: true, short: d

    def run
      DB.open(flags.database) do |db|
        pp db.exec(SQL_SCHEMA)
        pp db.exec(SQL_EXTENSIONS)
        pp db.exec(SQL_CREATE_EVENTS_TABLE)
        pp db.exec(SQL_CREATE_INTERNAL_EVENTS_TABLE)
        pp db.exec(SQL_FN_HASH64)
        pp db.exec(SQL_FN_READ_CATEGORY)
        pp db.exec(SQL_FN_EVENT_CATEGORY)
        pp db.exec(SQL_FN_READ_INTERNAL_STREAM)
        SQL_INDICES.each do |str|
          db.exec(str)
        end
      end
    end
  end
end
