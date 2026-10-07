begin transaction isolation level serializable;

alter table advisor_relations drop constraint advisor_relations_pkey;
alter table school_locations drop constraint school_locations_pkey;

commit
