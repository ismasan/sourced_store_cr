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

    SQL_CREATE_CONSUMERS_TABLE = <<-SQL
    CREATE TABLE IF NOT EXISTS event_store.consumers (
      id character varying NOT NULL,
      group_name character varying,
      last_seq bigint NOT NULL DEFAULT '0'::bigint,
      run_at timestamp without time zone NOT NULL DEFAULT now(),
      position integer DEFAULT 0,
      group_size integer NOT NULL DEFAULT 1
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

    SQL_FN_CHECKOUT_CONSUMER = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.checkout_consumer(
      group_name varchar,
      consumer_id varchar,
      last_seq bigint
    )
    RETURNS void
    AS $$
    DECLARE
    BEGIN
      UPDATE event_store.consumers c SET last_seq = checkout_consumer.last_seq, run_at = NOW()
        WHERE c.group_name = checkout_consumer.group_name
        AND c.id = checkout_consumer.consumer_id;
    END;
    $$ LANGUAGE plpgsql
    VOLATILE;
    SQL

    SQL_FN_CHECKIN_CONSUMER = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.checkin_consumer(
      group_name varchar,
      consumer_id varchar
    )
    RETURNS TABLE(
      pos integer,
      group_size integer,
      last_seq bigint
    )
    AS $$
    DECLARE
      current_consumer_count integer;
      group_counts record;
      current_group_size integer;
      current_group_min_seq bigint;
    BEGIN
     current_group_size := 0;
     current_group_min_seq := 0;

     SELECT COUNT(*) INTO current_consumer_count FROM event_store.consumers c WHERE c.group_name = checkin_consumer.group_name AND c.id = checkin_consumer.consumer_id;

     IF current_consumer_count > 0 THEN -- returning consumer
      raise notice 'consumer exists: do nothing';
     ELSE -- new consumer
      raise notice 'consumer does not exist: rebalance';
      SELECT COUNT(c.id) AS group_size, MIN(c.last_seq) AS group_min_seq INTO group_counts FROM event_store.consumers c WHERE c.group_name = checkin_consumer.group_name GROUP BY c.group_name;
      raise notice 'Inserting new consumer, rebalancing all consumers. group_size:% group_min_seq:%', group_counts.group_size, group_counts.group_min_seq;

      IF group_counts.group_size IS NOT NULL THEN
        current_group_size := group_counts.group_size;
        current_group_min_seq := group_counts.group_min_seq;
      END IF;

      INSERT INTO event_store.consumers(group_name, id, last_seq, run_at, group_size) VALUES(checkin_consumer.group_name, checkin_consumer.consumer_id, current_group_min_seq, NOW(), current_group_size);

      -- Update consumers with position and group_size
      UPDATE event_store.consumers c SET position = c2.position, group_size = current_group_size + 1, last_seq = current_group_min_seq
      FROM (SELECT c2.id, (ROW_NUMBER() OVER(ORDER BY id ASC) - 1) AS position FROM event_store.consumers c2) c2
      WHERE c.group_name = checkin_consumer.group_name AND c2.id = c.id;

     END IF;

     RETURN QUERY
     SELECT c.position AS pos, c.group_size, c.last_seq
     FROM event_store.consumers c
     WHERE c.group_name = checkin_consumer.group_name AND c.id = checkin_consumer.consumer_id;
    END;
    $$ LANGUAGE plpgsql
    VOLATILE;
    SQL

    SQL_FN_READ_CATEGORY = <<-SQL
    CREATE OR REPLACE FUNCTION event_store.read_category(
      cat varchar,
      group_name varchar,
      consumer_id varchar,
      lim integer
    )
    RETURNS SETOF event_store.events
    AS $$
    DECLARE
    BEGIN
      RETURN QUERY
        WITH current_consumer AS (SELECT * FROM event_store.checkin_consumer(read_category.group_name, read_category.consumer_id))
        SELECT event_store.events.*
        FROM event_store.events, current_consumer
        WHERE event_store.event_category(topic) = read_category.cat
              AND MOD(event_store.hash_64(stream_id::varchar), current_consumer.group_size) = current_consumer.pos
              AND global_seq > current_consumer.last_seq
              ORDER BY global_seq ASC
              LIMIT read_category.lim;
    END;
    $$ LANGUAGE plpgsql
    VOLATILE;
    SQL

    # Crystal's DB#exec can only take single commands :(
    SQL_INDICES = [
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_ids ON event_store.events (id)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_seqs ON event_store.events (stream_id, seq)),
      %(CREATE INDEX IF NOT EXISTS index_on_event_categories ON event_store.events (event_store.event_category(topic))),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_consumer_ids ON event_store.consumers(group_name text_ops,id text_ops);),
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
        pp db.exec(SQL_CREATE_CONSUMERS_TABLE)
        pp db.exec(SQL_FN_HASH64)
        pp db.exec(SQL_FN_CHECKIN_CONSUMER)
        pp db.exec(SQL_FN_CHECKOUT_CONSUMER)
        pp db.exec(SQL_FN_READ_CATEGORY)
        pp db.exec(SQL_FN_EVENT_CATEGORY)
        SQL_INDICES.each do |str|
          db.exec(str)
        end
      end
    end
  end
end
