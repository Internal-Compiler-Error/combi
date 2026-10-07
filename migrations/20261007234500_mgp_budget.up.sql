-- One token bucket for every request to MGP, from any crawl, walk or search: it refills at the
-- rate in worker/mgp-budget.ts and a request spends a token, so MGP's load has one overall cap.
create table mgp_budget (
    id      boolean primary key default true check (id),
    tokens  real not null,
    updated timestamptz not null
);
insert into mgp_budget (tokens, updated) values (0, now());
