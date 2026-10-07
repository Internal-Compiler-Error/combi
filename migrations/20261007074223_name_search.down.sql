begin transaction isolation level serializable;

drop index advisor_relations_advisee_idx;
alter table mathematicians drop column search_name;
drop function immutable_unaccent(text);

commit
