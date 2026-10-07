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
};

export type CrawlResult = {
  /** "fresh": crawled within RECRAWL_AFTER_DAYS, so the page was not fetched again */
  status: "crawled" | "fresh";
  last_crawled: string;
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

export type ApiErrorBody = { error: string };

export const MAX_GRAPH_DEPTH = 6;
export const MAX_GRAPH_NODES = 1500;
export const MAX_SCHOOL_PEOPLE = 2000;
/** a page crawled more recently than this is not fetched again */
export const RECRAWL_AFTER_DAYS = 14;

export const mgpUrl = (id: number) => `https://www.mathgenealogy.org/id.php?id=${id}`;
