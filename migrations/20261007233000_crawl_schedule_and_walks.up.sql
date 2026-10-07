begin transaction isolation level serializable;

-- When each page is next worth fetching. The interval adapts: it doubles when a crawl finds the
-- page unchanged and halves when it has changed, so busy pages are checked often and settled ones rarely.
create table crawl_schedule (
    page          int primary key,
    last_crawled  timestamptz not null,
    next_due      timestamptz not null,
    interval_days real not null,
    -- of what the page said, so the next crawl can tell whether anything changed
    content_hash  text,
    crawls        int not null default 1,
    changes       int not null default 0
);

-- pages crawled before there was a schedule are due two weeks after their last crawl
insert into crawl_schedule (page, last_crawled, next_due, interval_days)
select page_scraped, max(date), max(date) + interval '14 days', 14
from scrape_logs
where result = 'success'
group by page_scraped;

-- A crawl started from someone's page walks their students and advisors in the background.
create table crawl_walks (
    id       bigint generated always as identity primary key,
    root     int not null,
    started  timestamptz not null default now(),
    finished timestamptz,
    -- running, done, or capped when it reached the fetch limit first
    status   text not null default 'running',
    fetched  int not null default 0,
    -- not due yet, so known from the database without fetching
    skipped  int not null default 0,
    failed   int not null default 0
);
create index crawl_walks_root_idx on crawl_walks (root, started desc);

-- each walk's breadth-first frontier; `direction` says which way to keep going from a page
create table crawl_frontier (
    walk      bigint not null references crawl_walks on delete cascade,
    page      int not null,
    depth     int not null,
    direction text not null check (direction in ('both', 'down', 'up')),
    done      boolean not null default false,
    primary key (walk, page)
);
create index crawl_frontier_todo_idx on crawl_frontier (walk, depth, page) where not done;

commit
