import { useQuery } from "@tanstack/react-query";
import { geoArea, geoEqualEarth, geoPath, type GeoProjection } from "d3-geo";
import { feature } from "topojson-client";
import type { Feature, FeatureCollection, Geometry } from "geojson";
import type { GeometryCollection, Topology } from "topojson-specification";
import worldUrl from "world-atlas/countries-110m.json?url";
import type { CountryStat } from "../api/client";
import { DOTS, letters, shapeKey } from "./countries";

export type Shape = Feature<Geometry, { name: string }>;

/** Natural Earth's 1:110m countries, fetched on first use (it's ~100 KB) and kept for the session. */
export const useWorld = () =>
  useQuery({
    queryKey: ["world"],
    staleTime: Infinity,
    queryFn: async () => {
      const topo = (await (await fetch(worldUrl)).json()) as Topology<{ countries: GeometryCollection<{ name: string }> }>;
      const all = feature(topo, topo.objects.countries) as FeatureCollection<Geometry, { name: string }>;
      // Antarctica has no schools and would take a fifth of the map's height
      return all.features.filter((f) => f.properties.name !== "Antarctica");
    },
  });

/** One place on the map: a country shape or a dot, with every MGP country drawn there. */
export type Region = {
  key: string;
  name: string;
  /** largest first; Spain and Catalonia are both drawn as Spain */
  parts: CountryStat[];
  mathematicians: number;
  schools: number;
  shape?: Shape;
  dot?: [number, number];
};

export const MAP_WIDTH = 960;

/** The world in Equal Earth, scaled to MAP_WIDTH; height is however tall that makes it. */
export function fitWorld(shapes: Shape[]) {
  const collection = { type: "FeatureCollection" as const, features: shapes };
  const projection = geoEqualEarth().fitWidth(MAP_WIDTH, collection);
  const path = geoPath(projection);
  const [, [, bottom]] = path.bounds(collection);
  return { projection, path, height: Math.ceil(bottom) + 2 };
}

/**
 * Where on the map a region's arcs start and end: the middle of its largest landmass, so
 * France sits in Europe rather than between it and French Guiana.
 */
export function anchorOf(region: Region, projection: GeoProjection): [number, number] | null {
  if (region.dot) return projection(region.dot);
  const g = region.shape!.geometry;
  const polygon =
    g.type === "MultiPolygon"
      ? g.coordinates.map((coordinates) => ({ type: "Polygon" as const, coordinates })).sort((x, y) => geoArea(y) - geoArea(x))[0]!
      : g;
  const [x, y] = geoPath(projection).centroid(polygon);
  return Number.isFinite(x) ? [x, y] : null;
}

/** Where an MGP country is drawn: its shape, or a dot for places too small to have one. */
export function locate(country: string, shapes: Shape[]): Pick<Region, "key" | "shape" | "dot"> | null {
  const key = shapeKey(country);
  const shape = shapes.find((s) => letters(s.properties.name) === key);
  const dot = shape ? undefined : DOTS[country];
  return shape || dot ? { key, shape, dot } : null;
}

export function regionsFor(countries: CountryStat[], shapes: Shape[]) {
  const byKey = new Map(shapes.map((s) => [letters(s.properties.name), s]));
  const regions = new Map<string, Region>();
  const unplaced: CountryStat[] = [];
  for (const c of countries) {
    const key = shapeKey(c.country);
    const shape = byKey.get(key);
    const dot = shape ? undefined : DOTS[c.country];
    if (!shape && !dot) {
      unplaced.push(c);
      continue;
    }
    // countries arrive largest first, so a region is named after its largest part
    const r = regions.get(key) ?? { key, name: c.name, parts: [], mathematicians: 0, schools: 0, shape, dot };
    r.parts.push(c);
    r.mathematicians += c.mathematicians;
    r.schools += c.schools;
    regions.set(key, r);
  }
  return { regions, unplaced };
}

/**
 * Class breaks for a skewed count: four thresholds evenly spaced on a log scale up to `max`,
 * each rounded to a 1-2-5 number, so the legend reads "1–4, 5–49, 50–199, …".
 */
export function breaks(max: number): number[] {
  const nice = (v: number) => {
    const p = 10 ** Math.floor(Math.log10(v));
    return [1, 2, 5, 10].map((m) => m * p).reduce((best, n) => (Math.abs(Math.log(n / v)) < Math.abs(Math.log(best / v)) ? n : best));
  };
  const out: number[] = [];
  for (let i = 1; i <= 4; i++) {
    const t = nice(max ** (i / 5));
    if (t > (out.at(-1) ?? 1)) out.push(t);
  }
  return out;
}

/** Which of the five shades (1–5) a count falls in. */
export const shadeOf = (n: number, thresholds: number[]) => 1 + thresholds.filter((t) => n >= t).length;
