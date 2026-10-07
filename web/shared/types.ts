// Types shared by the API (worker/) and the web app (src/). Both sides import this file, so a
// change here is checked against every producer and consumer by `npm run typecheck`.

/** One mathematician as it appears in lists, search results and graphs. */
export type Person = {
  id: number;
  name: string;
  year: number | null;
  school: string | null;
  /** for /schools/{id}; null when the school is unknown */
  school_id: number | null;
  country: string | null;
  /** students recorded in the database, which may be fewer than the site lists */
  student_count: number;
};

export type PersonDetail = Person & {
  dissertation: string | null;
  advisors: Person[];
  students: Person[];
  /** all descendants reachable in the database, counted once each */
  descendant_count: number;
  /** ISO 8601 time their own page was last crawled; null when only other pages mention them */
  last_crawled: string | null;
  /** when their page is next worth fetching (see crawl_schedule); null when never crawled */
  next_crawl: string | null;
  /** the latest walk started from them in the last day, if any */
  walk: WalkStatus | null;
};

/** A background crawl through someone's students and advisors, started by their crawl button. */
export type WalkStatus = {
  id: number;
  root: number;
  /** "capped" stopped at MAX_WALK_FETCHES */
  status: "running" | "done" | "capped";
  /** pages fetched from MGP */
  fetched: number;
  /** pages not due for a recrawl, followed through the database instead */
  skipped: number;
  /** pages MGP doesn't have */
  failed: number;
  /** pages still to visit */
  todo: number;
};

export type CrawlResult = {
  /** "fresh": the page isn't due for a recrawl, so it wasn't fetched again */
  status: "crawled" | "fresh";
  last_crawled: string;
  next_crawl: string;
  /** whether the page differed from the last crawl; null on a first crawl */
  changed: boolean | null;
  /** the walk through their tree, when the deployment has a crawl queue */
  walk: WalkStatus | null;
};

export type GraphNode = Person & {
  /** generations from the focus: negative for advisors, positive for students */
  depth: number;
};

export type GraphLink = { advisor: number; student: number };

export type Graph = {
  focus: number;
  nodes: GraphNode[];
  links: GraphLink[];
  /** true when the neighbourhood had more than MAX_GRAPH_NODES people and the farthest were dropped */
  truncated: boolean;
};

export type GraphParams = {
  /** generations of advisors to include (0–6, default 2) */
  up?: number;
  /** generations of students to include (0–6, default 2) */
  down?: number;
};

export type Stats = {
  mathematicians: number;
  relations: number;
  countries: number;
  first_year: number | null;
  last_year: number | null;
  /** ISO 8601 time of the most recent successful page scrape */
  last_scraped: string | null;
};

/** One match from the Mathematics Genealogy Project's own search, whether or not we have them. */
export type MgpHit = {
  id: number;
  name: string;
  school: string | null;
  year: number | null;
  /** whether they are in our database at all, even if only as someone's advisor or student */
  known: boolean;
  /** when their own page was last crawled, as in PersonDetail */
  last_crawled: string | null;
  next_crawl: string | null;
};

/** Mathematicians by the country of the school they graduated from. */
export type CountryStat = {
  /** MGP's name for the country, as in its flag images ("UnitedStates"); the key for /countries/{country}/schools */
  country: string;
  /** the same, spelled for people ("United States") */
  name: string;
  mathematicians: number;
  schools: number;
};

export type SchoolStat = { id: number; school: string; mathematicians: number };

export type SchoolDetail = {
  id: number;
  name: string;
  countries: { country: string; name: string }[];
  mathematicians: number;
  first_year: number | null;
  last_year: number | null;
  /** degrees per decade, oldest first; only decades with any */
  decades: { decade: number; count: number }[];
  /** graduates, newest first, at most MAX_SCHOOL_PEOPLE */
  people: Person[];
};

export type SchoolHit = { id: number; name: string; country: string | null; mathematicians: number };

/** Advisor–student links that cross borders: advisor's degree country to the student's. */
export type Flows = {
  /** MGP country names, as in CountryStat.country */
  flows: { from: string; to: string; count: number }[];
  /** cross-border links per decade of the student's degree, over all time, for the time slider */
  decades: { decade: number; count: number }[];
};

/** How two mathematicians are related through their advisors. */
export type Relation = {
  a: Person;
  b: Person;
  /** their nearest shared academic ancestor, which may be a or b; null when the data links them nowhere */
  ancestor: Person | null;
  /** ancestor first, down to a; empty when unrelated */
  pathA: Person[];
  /** ancestor first, down to b */
  pathB: Person[];
};

export type ApiErrorBody = { error: string };

export const MAX_GRAPH_DEPTH = 6;
export const MAX_GRAPH_NODES = 1500;
export const MAX_SCHOOL_PEOPLE = 2000;
/** how many generations up the relation finder looks from each person */
export const MAX_RELATION_DEPTH = 60;
/** a page's recrawl interval stays within these; it halves when a crawl finds changes and doubles when not */
export const MIN_RECRAWL_DAYS = 3;
export const MAX_RECRAWL_DAYS = 365;
/** an ID MGP said it doesn't have is asked about again after this long */
export const NOT_FOUND_RECHECK_DAYS = 30;
/** a walk stops after fetching this many pages */
export const MAX_WALK_FETCHES = 2000;

export const mgpUrl = (id: number) => `https://www.mathgenealogy.org/id.php?id=${id}`;
