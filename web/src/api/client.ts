import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ApiErrorBody, CountryStat, CrawlResult, Flows, Graph, GraphParams, MgpHit, Person, PersonDetail, Relation, SchoolDetail, SchoolHit, SchoolStat, Stats, WalkStatus } from "../../shared/types";

export type { CountryStat, CrawlResult, Flows, Graph, GraphNode, GraphLink, MgpHit, Person, PersonDetail, Relation, SchoolDetail, SchoolHit, SchoolStat, Stats, WalkStatus } from "../../shared/types";
export { MAX_SCHOOL_PEOPLE, mgpUrl } from "../../shared/types";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

async function request<T>(method: "GET" | "POST", path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const url = new URL(`/api${path}`, window.location.origin);
  for (const [k, v] of Object.entries(params ?? {})) if (v !== undefined) url.searchParams.set(k, String(v));
  const res = await fetch(url, { method });
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as ApiErrorBody | null;
    throw new ApiError(body?.error ?? `Request failed (${res.status})`, res.status);
  }
  return res.json() as Promise<T>;
}

const get = <T>(path: string, params?: Record<string, string | number | undefined>) => request<T>("GET", path, params);

export const useStats = () => useQuery({ queryKey: ["stats"], queryFn: () => get<Stats>("/stats") });

export const useNotable = () => useQuery({ queryKey: ["notable"], queryFn: () => get<Person[]>("/notable") });

export const useSearch = (q: string, limit = 8) =>
  useQuery({
    queryKey: ["search", q, limit],
    queryFn: () => get<Person[]>("/search", { q, limit }),
    enabled: q.trim().length > 0,
    placeholderData: keepPreviousData,
  });

/** Searches MGP itself, which can reach people we have never crawled. Off until `enabled`, since
 * every search is a request to MGP. */
export const useMgpSearch = (q: string, enabled: boolean) =>
  useQuery({ queryKey: ["mgp-search", q], queryFn: () => get<MgpHit[]>("/mgp/search", { q }), enabled: enabled && q.trim().length > 0 });

export const useCountries = () => useQuery({ queryKey: ["countries"], queryFn: () => get<CountryStat[]>("/countries") });

export const useCountrySchools = (country: string | null) =>
  useQuery({
    queryKey: ["country-schools", country],
    queryFn: () => get<SchoolStat[]>(`/countries/${encodeURIComponent(country!)}/schools`),
    enabled: country !== null,
  });

export const useSchool = (id: number) => useQuery({ queryKey: ["school", id], queryFn: () => get<SchoolDetail>(`/schools/${id}`) });

export const useSchoolSearch = (q: string, limit = 8) =>
  useQuery({
    queryKey: ["school-search", q, limit],
    queryFn: () => get<SchoolHit[]>("/schools/search", { q, limit }),
    enabled: q.trim().length > 0,
    placeholderData: keepPreviousData,
  });

/** Cross-border advisor links, for one decade of students' degrees or (null) all time. */
export const useFlows = (decade: number | null) =>
  useQuery({
    queryKey: ["flows", decade],
    queryFn: () => get<Flows>("/flows", decade === null ? {} : { from: decade, to: decade + 9 }),
    placeholderData: keepPreviousData,
  });

export const useRelation = (a: number | null, b: number | null) =>
  useQuery({ queryKey: ["relation", a, b], queryFn: () => get<Relation>("/relation", { a: a!, b: b! }), enabled: a !== null && b !== null });

export const usePeople = (ids: number[]) =>
  useQuery({ queryKey: ["people", ids], queryFn: () => get<Person[]>("/people", { ids: ids.join(",") }), enabled: ids.length > 0, placeholderData: keepPreviousData });

/** Fresh people on every `draw`, so Shuffle brings new ones. */
export const useRandomPeople = (n: number, draw: number, enabled: boolean) =>
  useQuery({ queryKey: ["random", n, draw], queryFn: () => get<Person[]>("/random", { n }), enabled, staleTime: Infinity, placeholderData: keepPreviousData });

export const usePerson = (id: number, enabled = true) =>
  useQuery({ queryKey: ["person", id], queryFn: () => get<PersonDetail>(`/mathematicians/${id}`), enabled });

export const useGraph = (id: number, params: Required<GraphParams>) =>
  useQuery({
    queryKey: ["graph", id, params],
    queryFn: () => get<Graph>(`/mathematicians/${id}/graph`, params),
    placeholderData: keepPreviousData,
  });

/** Fetch someone's page from MGP into the database and start walking their tree, then refresh
 * everything: the crawl can add people and links that show up on other pages too. */
export function useCrawl(id: number) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: () => request<CrawlResult>("POST", `/mathematicians/${id}/crawl`),
    onSuccess: () => client.invalidateQueries(),
  });
}

/** A walk's progress, polled every couple of seconds while it runs. */
export const useWalk = (initial: WalkStatus | null) =>
  useQuery({
    queryKey: ["walk", initial?.id],
    queryFn: () => get<WalkStatus>(`/walks/${initial!.id}`),
    enabled: initial !== null,
    initialData: initial ?? undefined,
    staleTime: 0,
    refetchInterval: (q) => (q.state.data?.status === "running" ? 2000 : false),
  });

/** "Ph.D. 1963 · California Institute of Technology" */
export function degreeLine(p: Pick<Person, "year" | "school">): string {
  return [p.year ? `Ph.D. ${p.year}` : "Year unknown", p.school].filter(Boolean).join(" · ");
}
