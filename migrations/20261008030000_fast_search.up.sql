-- one- and two-letter searches have no trigrams to use, so they match the start of names instead
create index mathematicians_search_name_prefix_idx on mathematicians (search_name text_pattern_ops);
create index schools_search_name_prefix_idx on schools (search_name text_pattern_ops);

-- pg_trgm's default of 0.6 misses one-letter typos in short names. Set for the whole database so
-- a search is one statement, not a transaction around `set local` (four round trips via Hyperdrive).
-- Calling a pg_trgm function first loads its library, which a role without superuser (as on
-- Neon) needs before it may set one of its parameters.
do $$ begin
    perform word_similarity('a', 'a');
    execute format('alter database %I set pg_trgm.word_similarity_threshold = 0.45', current_database());
end $$;
