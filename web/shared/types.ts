// Types shared by the API (worker/) and the web app (src/). Both sides import this file, so a
// change here is checked against every producer and consumer by `npm run typecheck`.

/** One mathematician as it appears in lists, search results and graphs. */
export type Person = {
  id: number;
  name: string;
  year: number | null;
  school: string | null;
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

export type ApiErrorBody = { error: string };

export const MAX_GRAPH_DEPTH = 6;
export const MAX_GRAPH_NODES = 1500;
