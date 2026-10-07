import { useEffect, useMemo, useRef, useState } from "react";
import { stratify, tree, type HierarchyPointNode } from "d3-hierarchy";
import { linkRadial } from "d3-shape";
import { scaleSqrt } from "d3-scale";
import { select } from "d3-selection";
import "d3-transition"; // adds .transition() to selections
import { zoom as d3zoom, zoomIdentity, type ZoomBehavior } from "d3-zoom";
import type { Graph, GraphNode } from "../api/client";

/** distance between generation rings, in layout units */
const RING = 120;
/** categorical slots, in fixed order; every other country is drawn as a hollow ring */
const SLOTS = ["var(--c1)", "var(--c2)", "var(--c3)"] as const;

type Pos = { a: number; r: number; x: number; y: number };
type Link = { advisor: number; student: number; d: string; primary: boolean };
type Placed = { node: GraphNode; pos: Pos; desc: number; radius: number; color: string | null; labelTier: 0 | 1 | 2 };

function shortName(name: string) {
  const p = name.split(/\s+/);
  return p.length > 2 ? `${p[0]} ${p[p.length - 1]}` : name;
}

/**
 * Lay the focus's descendants out as a radial tree, one ring per generation. The data is a DAG
 * (people can have two advisors), so each person hangs off the advisor nearest the focus and any
 * other advisor link is drawn separately as a dashed curve.
 */
function layout(graph: Graph) {
  const below = graph.nodes.filter((n) => n.depth >= 0);
  const byId = new Map(below.map((n) => [n.id, n]));
  const students = new Map<number, number[]>();
  const advisors = new Map<number, number[]>();
  for (const l of graph.links) {
    if (!byId.has(l.advisor) || !byId.has(l.student)) continue;
    students.set(l.advisor, [...(students.get(l.advisor) ?? []), l.student]);
    advisors.set(l.student, [...(advisors.get(l.student) ?? []), l.advisor]);
  }

  // breadth-first from the focus: the first advisor to reach someone becomes their tree parent
  const parent = new Map<number, number>();
  const seen = new Set([graph.focus]);
  for (const queue = [graph.focus]; queue.length; ) {
    const id = queue.shift()!;
    for (const s of students.get(id) ?? []) if (!seen.has(s)) (seen.add(s), parent.set(s, id), queue.push(s));
  }
  const inTree = below.filter((n) => seen.has(n.id));

  // descendants within this view, each counted once
  const descMemo = new Map<number, Set<number>>();
  const desc = (id: number): Set<number> => {
    let set = descMemo.get(id);
    if (set) return set;
    set = new Set();
    for (const s of students.get(id) ?? []) {
      set.add(s);
      for (const d of desc(s)) set.add(d);
    }
    descMemo.set(id, set);
    return set;
  };

  const root = stratify<GraphNode>()
    .id((d) => String(d.id))
    .parentId((d) => (d.id === graph.focus ? null : String(parent.get(d.id))))(inTree);
  root.sort((a, b) => desc(b.data.id).size - desc(a.data.id).size || (a.data.year ?? 9999) - (b.data.year ?? 9999));
  const maxGen = Math.max(1, root.height);
  const laid = tree<GraphNode>()
    .size([2 * Math.PI, RING * maxGen])
    .separation((a, b) => (a.parent === b.parent ? 1 : 1.6) / Math.max(1, a.depth))(root);

  const pos = new Map<number, Pos>();
  laid.each((d: HierarchyPointNode<GraphNode>) =>
    pos.set(d.data.id, { a: d.x, r: d.y, x: d.y * Math.cos(d.x - Math.PI / 2), y: d.y * Math.sin(d.x - Math.PI / 2) }),
  );

  // colour the three most common countries in this view; legend says which is which
  const counts = new Map<string, number>();
  for (const n of inTree) if (n.country) counts.set(n.country, (counts.get(n.country) ?? 0) + 1);
  const top = [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, SLOTS.length).map(([c]) => c);
  const colorOf = (c: string | null) => (c && top.includes(c) ? SLOTS[top.indexOf(c)]! : null);

  const rootDesc = desc(graph.focus).size;
  const radius = scaleSqrt().domain([0, Math.max(1, rootDesc)]).range([4, 22]);
  const placed: Placed[] = inTree.map((n) => {
    const d = desc(n.id).size;
    // tier 0 is always labelled, 1 from moderate zoom, 2 only when zoomed in close
    const labelTier = n.id === graph.focus || d >= 8 ? 0 : d >= 3 ? 1 : 2;
    return { node: n, pos: pos.get(n.id)!, desc: d, radius: radius(d), color: colorOf(n.country), labelTier };
  });

  const radial = linkRadial<{ source: Pos; target: Pos }, Pos>()
    .angle((p) => p.a)
    .radius((p) => p.r);
  const links: Link[] = [];
  for (const [advisor, list] of students)
    for (const student of list) {
      const s = pos.get(advisor);
      const t = pos.get(student);
      if (!s || !t) continue;
      const primary = parent.get(student) === advisor;
      // a second advisor's link bows toward the centre so it reads apart from the branches
      const d = primary
        ? (radial({ source: s, target: t }) ?? "")
        : `M${s.x},${s.y}Q${((s.x + t.x) / 2) * 0.55},${((s.y + t.y) / 2) * 0.55} ${t.x},${t.y}`;
      links.push({ advisor, student, d, primary });
    }

  const lineage = (id: number) => {
    const out = [id];
    while (parent.has(out[0]!)) out.unshift(parent.get(out[0]!)!);
    return out;
  };

  return { placed, links, maxGen, top, desc, lineage, advisors, hasSecond: links.some((l) => !l.primary) };
}

export function RadialGraph({ graph, onOpen }: { graph: Graph; onOpen: (id: number) => void }) {
  const L = useMemo(() => layout(graph), [graph]);
  const wrapRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const viewRef = useRef<SVGGElement>(null);
  const zoomRef = useRef<ZoomBehavior<SVGSVGElement, unknown>>(null);
  const [hover, setHover] = useState<{ id: number; x: number; y: number } | null>(null);

  // Zooming must not re-render React: it sets the transform, a label tier and a label size directly.
  const applyZoom = (k: number) => {
    const svg = svgRef.current;
    if (!svg) return;
    svg.dataset.tier = k >= 2.2 ? "2" : k >= 1.3 ? "1" : "0";
    svg.style.setProperty("--label-scale", String(1 / Math.sqrt(k)));
  };

  const fit = (animate: boolean) => {
    const svg = svgRef.current;
    if (!svg || !zoomRef.current) return;
    const { width: w, height: h } = svg.getBoundingClientRect();
    const R = RING * L.maxGen + 40;
    const k = Math.min(w, h) / (2 * R);
    const t = zoomIdentity.translate(w / 2, h / 2).scale(k);
    const sel = select(svg);
    (animate ? sel.transition().duration(400) : sel).call(zoomRef.current.transform as never, t);
  };

  useEffect(() => {
    const svg = svgRef.current!;
    const z = d3zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.15, 8])
      .on("zoom", (e) => {
        viewRef.current?.setAttribute("transform", e.transform.toString());
        applyZoom(e.transform.k);
      });
    zoomRef.current = z;
    select(svg).call(z).on("dblclick.zoom", null);
    return () => void select(svg).on(".zoom", null);
  }, []);

  useEffect(() => fit(false), [L]); // eslint-disable-line react-hooks/exhaustive-deps

  const zoomBy = (f: number) => {
    if (svgRef.current && zoomRef.current) select(svgRef.current).transition().duration(200).call(zoomRef.current.scaleBy as never, f);
  };

  // hovering someone lights up their line back to the focus, everyone below them, and their other advisors
  const lit = useMemo(() => {
    if (!hover || hover.id === graph.focus) return null;
    return new Set([...L.lineage(hover.id), ...L.desc(hover.id), ...(L.advisors.get(hover.id) ?? [])]);
  }, [hover, L, graph.focus]);

  // names forced on while hovering: the line back to the focus, direct students, other advisors
  const named = useMemo(() => {
    if (!hover || hover.id === graph.focus) return null;
    const students = L.links.filter((l) => l.advisor === hover.id).map((l) => l.student);
    return new Set([...L.lineage(hover.id), ...students, ...(L.advisors.get(hover.id) ?? [])]);
  }, [hover, L, graph.focus]);

  const hovered = hover ? L.placed.find((p) => p.node.id === hover.id) : undefined;
  const track = (id: number) => (e: React.PointerEvent) => {
    const r = wrapRef.current!.getBoundingClientRect();
    setHover({ id, x: e.clientX - r.left, y: e.clientY - r.top });
  };

  return (
    <div className="graph" ref={wrapRef}>
      <svg ref={svgRef} role="group" aria-label="Descendants arranged in rings, one ring per generation" data-tier="0">
        <g ref={viewRef}>
          <g className="rings">
            {Array.from({ length: L.maxGen }, (_, i) => (
              <g key={i}>
                <circle className="ring" r={(i + 1) * RING} />
                <text className="ring-label" x={4} y={-(i + 1) * RING - 4}>
                  Gen {i + 1}
                </text>
              </g>
            ))}
          </g>
          <g>
            {L.links.map((l) => {
              const on = lit && lit.has(l.advisor) && lit.has(l.student);
              return <path key={`${l.advisor}-${l.student}`} d={l.d} className={`link ${l.primary ? "" : "link-second"} ${lit ? (on ? "is-lit" : "is-dim") : ""}`} />;
            })}
          </g>
          <g>
            {L.placed.map(({ node, pos, radius, color }) => {
              const focus = node.id === graph.focus;
              return (
                <circle
                  key={node.id}
                  cx={pos.x}
                  cy={pos.y}
                  r={radius}
                  className={`dot ${color ? "" : "dot-other"} ${focus ? "dot-focus" : ""} ${lit && !lit.has(node.id) ? "is-dim" : ""}`}
                  style={color ? { fill: color } : undefined}
                />
              );
            })}
          </g>
          <g>
            {L.placed.map(({ node, pos, radius, labelTier }) => {
              const focus = node.id === graph.focus;
              const shown = named?.has(node.id) ?? false;
              if (focus)
                return (
                  <text key={node.id} className="n-label n-label-focus" textAnchor="middle" y={radius + 16} dy="0.32em">
                    {shortName(node.name)}
                  </text>
                );
              const deg = (pos.a * 180) / Math.PI - 90;
              const flip = pos.a > Math.PI;
              const off = radius + 4;
              return (
                <text
                  key={node.id}
                  className={`n-label tier-${labelTier} ${shown ? "force" : ""} ${lit && !lit.has(node.id) ? "is-dim" : ""}`}
                  textAnchor={flip ? "end" : "start"}
                  dx={flip ? -off : off}
                  dy="0.32em"
                  transform={`translate(${pos.x},${pos.y}) rotate(${flip ? deg + 180 : deg})`}
                >
                  {shortName(node.name)}
                </text>
              );
            })}
          </g>
          <g>
            {L.placed.map(({ node, pos, radius }) => (
              <circle
                key={node.id}
                cx={pos.x}
                cy={pos.y}
                r={Math.max(10, radius + 4)}
                className="hit"
                role="link"
                tabIndex={0}
                aria-label={`${node.name}${node.year ? `, ${node.year}` : ""}`}
                onPointerEnter={track(node.id)}
                onPointerMove={track(node.id)}
                onPointerLeave={() => setHover(null)}
                onFocus={() => setHover({ id: node.id, x: 16, y: 16 })}
                onBlur={() => setHover(null)}
                onClick={() => onOpen(node.id)}
                onKeyDown={(e) => e.key === "Enter" && onOpen(node.id)}
              />
            ))}
          </g>
        </g>
      </svg>

      {hovered && hover && (
        <div className="tip" style={{ left: Math.min(hover.x + 14, (wrapRef.current?.clientWidth ?? 0) - 280), top: hover.y + 14 }}>
          <b>{hovered.node.name}</b>
          {hovered.node.year ?? "Year unknown"} · {hovered.node.school ?? "School unknown"}
          <br />
          {hovered.node.student_count} students · {hovered.desc} descendants shown
        </div>
      )}

      <div className="graph-zoom">
        <button type="button" onClick={() => zoomBy(1.5)} aria-label="Zoom in">
          +
        </button>
        <button type="button" onClick={() => zoomBy(1 / 1.5)} aria-label="Zoom out">
          −
        </button>
        <button type="button" onClick={() => fit(true)} aria-label="Fit whole tree">
          ⤢
        </button>
      </div>

      <div className="legend">
        {L.top.map((c, i) => (
          <span key={c}>
            <i className="swatch" style={{ background: SLOTS[i] }} />
            {c}
          </span>
        ))}
        <span>
          <i className="swatch swatch-other" />
          {L.top.length ? "Elsewhere" : "Country unknown"}
        </span>
        {L.hasSecond && (
          <span>
            <i className="dash" />
            Second advisor
          </span>
        )}
        <span>Size = descendants</span>
      </div>
    </div>
  );
}
