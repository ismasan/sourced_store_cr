require "admiral"
require "db"
require "pg"

module CLI
  class Setup < Admiral::Command
    SQL_FN_HASH64 = <<-SQL
    CREATE OR REPLACE FUNCTION public.hash_64(
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
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_event_ids ON public.events (id)),
      %(CREATE UNIQUE INDEX IF NOT EXISTS unique_index_on_entity_seqs ON public.events (stream_id, seq)),
      %(CREATE INDEX IF NOT EXISTS index_on_event_categories ON public.events (public.event_category(topic)))
    ]

    SQL_FN_EVENT_CATEGORY = <<-SQL
    CREATE OR REPLACE FUNCTION public.event_category(topic varchar) RETURNS varchar AS $$
    BEGIN
      RETURN SPLIT_PART(event_category.topic, '.', 1);
    END;
    $$ LANGUAGE plpgsql
    IMMUTABLE;
    SQL

    define_flag database : String, required: true, short: d

    def run
      DB.open(flags.database) do |db|
        pp db.exec(SQL_FN_HASH64)
        pp db.exec(SQL_FN_EVENT_CATEGORY)
        SQL_INDICES.each do |str|
          db.exec(str)
        end
      end
    end
  end
end
