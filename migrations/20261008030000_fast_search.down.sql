do $$ begin
    perform word_similarity('a', 'a');
    execute format('alter database %I reset pg_trgm.word_similarity_threshold', current_database());
end $$;
drop index schools_search_name_prefix_idx;
drop index mathematicians_search_name_prefix_idx;
