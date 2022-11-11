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
      global_seq BIGSERIAL NOT NULL,
      seq integer NOT NULL DEFAULT 0,
      created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,
      metadata json,
      payload json,
      CONSTRAINT pk_events_global_seq PRIMARY KEY (global_seq)
    );
    SQL

    SQL_CREATE_CATEGORIES_TABLE_SQL = <<-SQL
    CREATE TABLE IF NOT EXISTS event_store.categories (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        name character varying NOT NULL UNIQUE,
        created_at timestamp without time zone DEFAULT timezone('utc'::text, now())
    );
    SQL

    SQL_CREATE_CATEGORIES_TO_EVENTS_TABLE_SQL = <<-SQL
    CREATE TABLE IF NOT EXISTS event_store.categories_to_events (
        category_id uuid REFERENCES event_store.categories(id) ON DELETE CASCADE,
        event_id uuid REFERENCES event_store.events(id) ON DELETE CASCADE,
        created_at timestamp without time zone DEFAULT timezone('utc'::text, now())
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
    	categories text[];
    	categories_size int;
    BEGIN
    	categories := string_to_array(read_category.category, '.');
    	categories_size := array_length(categories, 1);

      RETURN QUERY
        SELECT
        	e.*
        FROM event_store.events e
        	LEFT JOIN event_store.categories_to_events ce ON ce.event_id = e.id
			    LEFT JOIN event_store.categories cats ON cats.id = ce.category_id
     	  WHERE cats.name = ANY (categories)
          AND MOD(event_store.hash_64(e.stream_id::varchar), read_category.group_size) = read_category.consumer_position
          AND e.global_seq > read_category.after_seq
        GROUP BY e.global_seq
		    HAVING count(*) = categories_size
        ORDER BY e.global_seq ASC
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

    SQL_PROC_COMPACT_STREAMS = <<-SQL
    CREATE OR REPLACE PROCEDURE event_store.compact_streams (snapshot_topic varchar, snapshots_to_keep integer)
    LANGUAGE SQL
    AS $$
      WITH snapshots AS (
        SELECT
          stream_id,
          topic,
          seq,
          rank() OVER (PARTITION BY stream_id ORDER BY seq DESC) AS seq_rank
        FROM
          event_store.internal_events
        WHERE
          topic = snapshot_topic)
      DELETE FROM event_store.internal_events e USING snapshots s
    WHERE s.stream_id = e.stream_id
      AND s.seq_rank = snapshots_to_keep
      AND e.seq < s.seq;
    $$;
    SQL

    # Crystal's DB#exec can only take single commands :(
    SQL_INDICES = [
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_ids ON event_store.events (id)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_seqs ON event_store.events (stream_id, seq)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS internal_events_pkey ON event_store.internal_events(id int4_ops)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_internal_event_seqs ON event_store.internal_events(stream_id text_ops,seq int8_ops)),
      %(CREATE INDEX IF NOT EXISTS index_on_internal_event_topics ON event_store.internal_events(topic)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS categories_pkey ON event_store.categories(id uuid_ops)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS categories_name_key ON event_store.categories(name text_ops)),
    ]

    define_flag database : String, required: true, short: d

    def run
      DB.open(flags.database) do |db|
        pp db.exec(SQL_SCHEMA)
        pp db.exec(SQL_EXTENSIONS)
        pp db.exec(SQL_CREATE_EVENTS_TABLE)
        pp db.exec(SQL_CREATE_INTERNAL_EVENTS_TABLE)
        pp db.exec(SQL_CREATE_CATEGORIES_TABLE_SQL)
        pp db.exec(SQL_CREATE_CATEGORIES_TO_EVENTS_TABLE_SQL)
        pp db.exec(SQL_FN_HASH64)
        pp db.exec(SQL_FN_READ_CATEGORY)
        pp db.exec(SQL_FN_READ_INTERNAL_STREAM)
        pp db.exec(SQL_PROC_COMPACT_STREAMS)
        SQL_INDICES.each do |str|
          db.exec(str)
        end
      end
    end
  end
end
