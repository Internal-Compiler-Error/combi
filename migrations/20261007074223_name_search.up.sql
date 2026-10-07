begin transaction isolation level serializable;

create extension if not exists unaccent;
create extension if not exists pg_trgm;

-- unaccent() is only STABLE because its dictionary can change; pinning the dictionary
-- lets us use it in a generated column and an index
create function immutable_unaccent(text) returns text
    language sql immutable parallel safe strict
    return public.unaccent('public.unaccent'::regdictionary, $1);

-- lower-cased, accent-free name: what search matches against ("Gödel" is found by "godel")
alter table mathematicians
    add column search_name text generated always as (lower(immutable_unaccent(name))) stored;

create index mathematicians_search_name_trgm on mathematicians using gin (search_name gin_trgm_ops);

-- the primary key covers advisor -> students; this covers student -> advisors
create index advisor_relations_advisee_idx on advisor_relations (advisee);

commit
