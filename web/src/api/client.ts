import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ApiErrorBody, CountryStat, CrawlResult, Graph, GraphParams, MgpHit, Person, PersonDetail, SchoolStat, Stats } from "../../shared/types";

export type { CountryStat, CrawlResult, Graph, GraphNode, GraphLink, MgpHit, Person, PersonDetail, SchoolStat, Stats } from "../../shared/types";
export { mgpUrl, RECRAWL_AFTER_DAYS } from "../../shared/types";

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

export const usePerson = (id: number) =>
  useQuery({ queryKey: ["person", id], queryFn: () => get<PersonDetail>(`/mathematicians/${id}`) });

export const useGraph = (id: number, params: Required<GraphParams>) =>
  useQuery({
    queryKey: ["graph", id, params],
    queryFn: () => get<Graph>(`/mathematicians/${id}/graph`, params),
    placeholderData: keepPreviousData,
  });

/** Fetch someone's page from MGP into the database, then refresh everything: the crawl can add
 * people and links that show up on other pages too. */
export function useCrawl(id: number) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: () => request<CrawlResult>("POST", `/mathematicians/${id}/crawl`),
    onSuccess: () => client.invalidateQueries(),
  });
}

/** "Ph.D. 1963 · California Institute of Technology" */
export function degreeLine(p: Pick<Person, "year" | "school">): string {
  return [p.year ? `Ph.D. ${p.year}` : "Year unknown", p.school].filter(Boolean).join(" · ");
}
