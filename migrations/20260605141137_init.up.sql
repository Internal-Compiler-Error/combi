create table mathematicians(
    id int primary key,
    name text
);

create table dissertations(
    title text not null,
    author int not null references mathematicians
);

create table schools(
    name text primary key
);

-- this definition has issues, school and year really should be merged to the mathematician table itself
-- 2026-06-05: it's been fixed in the next migration
create table graduation_records(
    mathematician int not null references mathematicians primary key,
    school text references schools not null,
    year int not null
);

create table advisor_relations(
    advisor int not null references mathematicians,
    advisee int not null references mathematicians
);

create table countries(
    name text primary key
);

create table school_locations(
    school text not null references schools,
    country text not null references countries
);

create table scrape_logs(
    id bigserial primary key,
    date timestamptz not null,
    page_scraped int not null,
    result text not null
);

create index scrape_logs_idx on scrape_logs(page_scraped, date desc, id desc);