begin transaction isolation level serializable;

alter table mathematicians
    add column dissertation    text,
    add column graduating_year int,
    add column school          text references schools;

UPDATE mathematicians
SET dissertation = d.title
FROM dissertations as d
WHERE mathematicians.id = d.author;

UPDATE mathematicians
SET graduating_year = g.year,
    school = g.school
FROM graduation_records as g
WHERE mathematicians.id = g.mathematician;

drop table graduation_records, dissertations;
commit