import { useMemo, useRef, useState, type CSSProperties } from "react";
import { scaleSqrt } from "d3-scale";
import { prettyCountry } from "../../shared/names";
import type { Flows } from "../api/client";
import { anchorOf, fitWorld, locate, MAP_WIDTH, type Region, type Shape } from "./world";

const fmt = new Intl.NumberFormat();
/** with nothing selected, only the biggest flows are drawn; past this the map turns to hair */
const MAX_ARCS = 100;

export type Arc = { id: string; from: string; to: string; fromName: string; toName: string; count: number };

/**
 * Advisor-country to student-country flows between map regions, largest first. Spain and
 * Catalonia are both Spain on the map, so links between them drop out here.
 */
export function arcsFor(flows: Flows["flows"], shapes: Shape[]) {
  const places = new Map<string, Pick<Region, "key" | "shape" | "dot"> | null>();
  const place = (c: string) => (places.has(c) ? places.get(c)! : (places.set(c, locate(c, shapes)), places.get(c)!));
  const arcs = new Map<string, Arc>();
  for (const f of flows) {
    const from = place(f.from);
    const to = place(f.to);
    if (!from || !to || from.key === to.key) continue;
    const id = `${from.key}>${to.key}`;
    const arc = arcs.get(id) ?? { id, from: from.key, to: to.key, fromName: prettyCountry(f.from), toName: prettyCountry(f.to), count: 0 };
    arc.count += f.count;
    arcs.set(id, arc);
  }
  const regions = new Map([...places.values()].filter((p) => p !== null).map((p) => [p.key, p]));
  return { arcs: [...arcs.values()].sort((a, b) => b.count - a.count), regions };
}

type Props = {
  shapes: Shape[];
  arcs: Arc[];
  regions: Map<string, Pick<Region, "key" | "shape" | "dot">>;
  selected: string | null;
  highlighted: string | null;
  /** changes whenever the arcs should draw themselves in again */
  drawKey: string;
  onSelect: (key: string | null) => void;
  onHighlight: (arcId: string | null) => void;
  period: string;
};

export function FlowMap({ shapes, arcs, regions, selected, highlighted, drawKey, onSelect, onHighlight, period }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [tip, setTip] = useState<{ x: number; y: number } | null>(null);
  const { path, projection, height } = useMemo(() => fitWorld(shapes), [shapes]);

  const anchors = useMemo(() => {
    const out = new Map<string, [number, number]>();
    for (const r of regions.values()) {
      const a = anchorOf(r as Region, projection);
      if (a) out.set(r.key, a);
    }
    return out;
  }, [regions, projection]);

  const byShape = useMemo(() => new Map([...regions.values()].filter((r) => r.shape).map((r) => [r.shape!, r])), [regions]);

  const shown = selected ? arcs.filter((a) => a.from === selected || a.to === selected) : arcs.slice(0, MAX_ARCS);
  const width = scaleSqrt()
    .domain([1, Math.max(1, ...shown.map((a) => a.count))])
    .range([0.75, 9]);
  const drawn = shown.flatMap((a, rank) => {
    const p0 = anchors.get(a.from);
    const p1 = anchors.get(a.to);
    if (!p0 || !p1) return [];
    const [x0, y0] = p0;
    const [x1, y1] = p1;
    const dx = x1 - x0;
    const dy = y1 - y0;
    const dist = Math.hypot(dx, dy) || 1;
    // bow to the left of travel, so A→B and B→A take different curves
    const cx = (x0 + x1) / 2 - (dy / dist) * dist * 0.22;
    const cy = (y0 + y1) / 2 + (dx / dist) * dist * 0.22;
    return [{ ...a, rank, d: `M${x0},${y0}Q${cx},${cy} ${x1},${y1}`, x0, y0, x1, y1, w: width(a.count) }];
  });
  // biggest on top, so small flows don't cut across them
  const ordered = [...drawn].reverse();
  const active = highlighted ? drawn.find((a) => a.id === highlighted) : undefined;
  const touching = new Set(drawn.flatMap((a) => [a.from, a.to]));

  const track = (id: string) => (e: React.PointerEvent) => {
    const box = wrapRef.current!.getBoundingClientRect();
    setTip({ x: e.clientX - box.left, y: e.clientY - box.top });
    onHighlight(id);
  };
  const clientWidth = wrapRef.current?.clientWidth ?? 0;

  return (
    <div className="map flowmap" ref={wrapRef} onPointerLeave={() => (setTip(null), onHighlight(null))}>
      <svg viewBox={`0 0 ${MAP_WIDTH} ${height}`} role="img" aria-label="World map with arcs from where advisors got their degrees to where their students got theirs; the list beside it has the same numbers">
        <defs>
          {drawn.map((a) => (
            <linearGradient key={a.id} id={`flow-${a.rank}`} gradientUnits="userSpaceOnUse" x1={a.x0} y1={a.y0} x2={a.x1} y2={a.y1}>
              <stop offset="0" className="flow-stop-from" />
              <stop offset="1" className="flow-stop-to" />
            </linearGradient>
          ))}
        </defs>
        <g>
          {shapes.map((s) => {
            const r = byShape.get(s);
            const live = r && touching.has(r.key);
            return (
              <path
                key={s.properties.name}
                d={path(s) ?? undefined}
                className={`flow-land ${live ? "is-live" : ""} ${r && r.key === selected ? "is-selected" : ""}`}
                onClick={() => live && onSelect(r.key === selected ? null : r.key)}
              />
            );
          })}
        </g>
        <g className="flow-arcs" key={drawKey}>
          {ordered.map((a) => (
            <path
              key={a.id}
              d={a.d}
              pathLength={1}
              className={`flow-arc ${active ? (active.id === a.id ? "is-lit" : "is-dim") : ""}`}
              stroke={`url(#flow-${a.rank})`}
              strokeWidth={a.w}
              style={{ "--i": Math.min(a.rank, 40) } as CSSProperties}
            />
          ))}
        </g>
        <g>
          {ordered.map((a) => (
            <path key={a.id} d={a.d} className="flow-hit" strokeWidth={Math.max(10, a.w + 6)} onPointerMove={track(a.id)} onClick={() => onSelect(a.to === selected ? a.from : a.to)} />
          ))}
        </g>
        <g>
          {[...touching].map((key) => {
            const p = anchors.get(key);
            return p && <circle key={key} cx={p[0]} cy={p[1]} r={key === selected ? 4.5 : 2.5} className={`flow-node ${key === selected ? "is-selected" : ""}`} />;
          })}
        </g>
      </svg>

      {tip && active && (
        <div className="tip" style={tip.x + 250 > clientWidth ? { right: clientWidth - tip.x + 14, top: tip.y + 14 } : { left: tip.x + 14, top: tip.y + 14 }}>
          <b>
            {active.fromName} → {active.toName}
          </b>
          {fmt.format(active.count)} student{active.count === 1 ? "" : "s"} of advisors from {active.fromName} · {period}
        </div>
      )}
    </div>
  );
}
