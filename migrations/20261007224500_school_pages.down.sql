begin transaction isolation level serializable;

drop index scrape_logs_success_idx;
drop index schools_search_name_trgm;
alter table schools drop column search_name;
drop index mathematicians_school_year_idx;
alter table schools drop column id;

commit
