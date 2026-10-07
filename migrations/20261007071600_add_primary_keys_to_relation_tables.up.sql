begin transaction isolation level serializable;

-- without a key, ON CONFLICT DO NOTHING never fires, so every rescrape duplicated rows
delete from advisor_relations a
    using advisor_relations b
where a.ctid > b.ctid
  and a.advisor = b.advisor
  and a.advisee = b.advisee;

delete from school_locations a
    using school_locations b
where a.ctid > b.ctid
  and a.school = b.school
  and a.country = b.country;

alter table advisor_relations add primary key (advisor, advisee);
alter table school_locations add primary key (school, country);

commit
