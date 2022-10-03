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

    SQL_CREATE_TABLE = <<-SQL
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

    # Crystal's DB#exec can only take single commands :(
    SQL_INDICES = [
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_ids ON event_store.events (id)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_seqs ON event_store.events (stream_id, seq)),
      %(CREATE INDEX IF NOT EXISTS index_on_event_categories ON event_store.events (event_store.event_category(topic))),
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
        pp db.exec(SQL_CREATE_TABLE)
        pp db.exec(SQL_FN_HASH64)
        pp db.exec(SQL_FN_EVENT_CATEGORY)
        SQL_INDICES.each do |str|
          db.exec(str)
        end
      end
    end
  end
end
