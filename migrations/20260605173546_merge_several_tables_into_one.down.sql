begin transaction isolation level serializable;

create table dissertations(
                              title text not null,
                              author int not null references mathematicians
);

create table graduation_records(
                                   mathematician int not null references mathematicians primary key,
                                   school text references schools not null,
                                   year int not null
);

insert into dissertations(title, author)
select dissertation, id
from mathematicians
where mathematicians.dissertation is not null;

insert into graduation_records(mathematician, school, year)
select id, school, graduating_year
from mathematicians
where mathematicians.school is not null and mathematicians.graduating_year is not null;

alter table mathematicians
    drop column dissertation,
    drop column graduating_year,
    drop column school;

commit