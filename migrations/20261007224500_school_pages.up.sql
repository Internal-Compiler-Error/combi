begin transaction isolation level serializable;

-- schools are keyed by name, which makes awkward URLs; a number names a school's page instead
alter table schools add column id int generated always as identity unique;

-- a school's graduates in year order, and the per-country and per-school counts
create index mathematicians_school_year_idx on mathematicians (school, graduating_year);

-- schools are searchable like people: lower-cased and accent-free, matched by trigrams
alter table schools
    add column search_name text generated always as (lower(immutable_unaccent(name))) stored;
create index schools_search_name_trgm on schools using gin (search_name gin_trgm_ops);

-- "when was X last crawled" (person pages, MGP search, the 14-day check) only ever asks about
-- successes, and failed rows are a third of the log
create index scrape_logs_success_idx on scrape_logs (page_scraped, date desc) where result = 'success';

commit
