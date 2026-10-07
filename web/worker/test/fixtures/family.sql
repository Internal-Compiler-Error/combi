-- A small slice of a real lineage, with one student who has two advisors (Klein)
-- and one person unconnected to the rest (Gödel).
insert into countries (name) values ('Germany'), ('Austria');
insert into schools (name) values ('Universität Helmstedt'), ('Universität Marburg'), ('Universität Bonn'), ('Universität Wien');
insert into school_locations (school, country) values
    ('Universität Helmstedt', 'Germany'), ('Universität Marburg', 'Germany'),
    ('Universität Bonn', 'Germany'), ('Universität Wien', 'Austria');

insert into mathematicians (id, name, dissertation, graduating_year, school) values
    (1, 'Carl Friedrich Gauß', 'Demonstratio nova theorematis', 1799, 'Universität Helmstedt'),
    (2, 'Christian Ludwig Gerling', 'Methodi proiectionis orthographicae', 1812, 'Universität Helmstedt'),
    (3, 'Julius Plücker', 'Generalem analyseos applicationem', 1823, 'Universität Marburg'),
    (4, 'C. Felix Klein', 'Über die Transformation der allgemeinen Gleichung', 1868, 'Universität Bonn'),
    (5, 'Rudolf Otto Sigismund Lipschitz', '   ', 1853, 'Universität Bonn'),
    (6, 'Kurt Gödel', 'Über die Vollständigkeit des Logikkalküls', 1929, 'Universität Wien'),
    (7, 'Ferdinand von Lindemann', null, 1873, null);

insert into advisor_relations (advisor, advisee) values
    (1, 2), (2, 3), (3, 4), (5, 4), (4, 7);

insert into scrape_logs (date, page_scraped, result) values
    ('2026-10-01T12:00:00Z', 1, 'success'),
    ('2026-10-02T12:00:00Z', 6, 'failed');
