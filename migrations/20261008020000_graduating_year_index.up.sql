-- the home page shows the earliest and latest degree years; this makes min/max a lookup, not a scan
create index mathematicians_graduating_year_idx on mathematicians (graduating_year);
