import { keepPreviousData, useQuery } from "@tanstack/react-query";
import type { Graph } from "./types/Graph";
import type { GraphParams } from "./types/GraphParams";
import type { Person } from "./types/Person";
import type { PersonDetail } from "./types/PersonDetail";
import type { Stats } from "./types/Stats";

export type { Graph, GraphNode, GraphLink, Person, PersonDetail, Stats };
import type { GraphLink } from "./types/GraphLink";
import type { GraphNode } from "./types/GraphNode";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

async function get<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const url = new URL(`/api${path}`, window.location.origin);
  for (const [k, v] of Object.entries(params ?? {})) if (v !== undefined) url.searchParams.set(k, String(v));
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new ApiError(body?.error ?? `Request failed (${res.status})`, res.status);
  }
  return res.json();
}

export const useStats = () => useQuery({ queryKey: ["stats"], queryFn: () => get<Stats>("/stats") });

export const useNotable = () => useQuery({ queryKey: ["notable"], queryFn: () => get<Person[]>("/notable") });

export const useSearch = (q: string, limit = 8) =>
  useQuery({
    queryKey: ["search", q, limit],
    queryFn: () => get<Person[]>("/search", { q, limit }),
    enabled: q.trim().length > 0,
    placeholderData: keepPreviousData,
  });

export const usePerson = (id: number) =>
  useQuery({ queryKey: ["person", id], queryFn: () => get<PersonDetail>(`/mathematicians/${id}`) });

export const useGraph = (id: number, params: Required<GraphParams>) =>
  useQuery({
    queryKey: ["graph", id, params],
    queryFn: () => get<Graph>(`/mathematicians/${id}/graph`, params),
    placeholderData: keepPreviousData,
  });

/** "Ph.D. 1963 · California Institute of Technology" */
export function degreeLine(p: Pick<Person, "year" | "school">): string {
  return [p.year ? `Ph.D. ${p.year}` : "Year unknown", p.school].filter(Boolean).join(" · ");
}

export const mgpUrl = (id: number) => `https://www.mathgenealogy.org/id.php?id=${id}`;
