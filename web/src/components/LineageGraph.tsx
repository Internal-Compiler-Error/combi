import { useEffect, useMemo, useRef, useState } from "react";
import { coordGreedy, coordSimplex, decrossTwoLayer, graphStratify, layeringLongestPath, layeringSimplex, sugiyama } from "d3-dag";
import { select } from "d3-selection";
import "d3-transition"; // adds .transition() to selections
import { zoom as d3zoom, zoomIdentity, type ZoomBehavior } from "d3-zoom";
import { curveMonotoneY, line } from "d3-shape";
import type { Graph, GraphNode } from "../api/client";

const CARD_W = 184;
const CARD_H = 46;
const GAP: [number, number] = [14, 54];
/** the optimal (simplex) passes get slow past a few hundred people; switch to greedy ones */
const FAST_LAYOUT_ABOVE = 250;
/** below this zoom the card text is unreadable, so fitting stops here */
const MIN_READABLE_SCALE = 0.6;

type Placed = { node: GraphNode; x: number; y: number };
type Edge = { advisor: number; student: number; d: string };

function clip(s: string, n: number) {
  return s.length > n ? s.slice(0, n - 1).trimEnd() + "…" : s;
}

function layout(graph: Graph): { width: number; height: number; placed: Placed[]; edges: Edge[] } {
  const ids = new Set(graph.nodes.map((n) => n.id));
  const parents = new Map<number, string[]>();
  for (const l of graph.links) {
    if (!ids.has(l.advisor) || !ids.has(l.student)) continue;
    parents.set(l.student, [...(parents.get(l.student) ?? []), String(l.advisor)]);
  }
  const dag = graphStratify()(graph.nodes.map((n) => ({ id: String(n.id), parentIds: parents.get(n.id) ?? [], node: n })));

  const size = [CARD_W, CARD_H] as const;
  const { width, height } =
    graph.nodes.length > FAST_LAYOUT_ABOVE
      ? sugiyama().layering(layeringLongestPath()).decross(decrossTwoLayer()).coord(coordGreedy()).nodeSize(size).gap(GAP)(dag)
      : sugiyama().layering(layeringSimplex()).decross(decrossTwoLayer()).coord(coordSimplex()).nodeSize(size).gap(GAP)(dag);

  const path = line().curve(curveMonotoneY);
  const placed = [...dag.nodes()].map((n) => ({ node: n.data.node, x: n.x, y: n.y }));
  const edges = [...dag.links()].map((l) => ({
    advisor: l.source.data.node.id,
    student: l.target.data.node.id,
    d: path(l.points) ?? "",
  }));
  return { width, height, placed, edges };
}

export function LineageGraph({ graph, onOpen }: { graph: Graph; onOpen: (id: number) => void }) {
  const { width, height, placed, edges } = useMemo(() => layout(graph), [graph]);
  const svgRef = useRef<SVGSVGElement>(null);
  const viewRef = useRef<SVGGElement>(null);
  const zoomRef = useRef<ZoomBehavior<SVGSVGElement, unknown>>(null);
  const [hover, setHover] = useState<number | null>(null);

  const fit = (animate: boolean) => {
    const svg = svgRef.current;
    if (!svg || !zoomRef.current) return;
    const { width: w, height: h } = svg.getBoundingClientRect();
    const pad = 24;
    const fitK = Math.min((w - pad * 2) / width, (h - pad * 2) / height);
    let t;
    if (fitK >= MIN_READABLE_SCALE) {
      const k = Math.min(1.25, fitK);
      t = zoomIdentity.translate((w - width * k) / 2, (h - height * k) / 2).scale(k);
    } else {
      // the whole neighbourhood would be too small to read: stay legible, centre on the focus, let the reader pan
      const f = placed.find((p) => p.node.id === graph.focus) ?? { x: width / 2, y: height / 2 };
      const k = MIN_READABLE_SCALE;
      t = zoomIdentity.translate(w / 2 - f.x * k, h / 2 - f.y * k).scale(k);
    }
    const sel = select(svg);
    (animate ? sel.transition().duration(400) : sel).call(zoomRef.current.transform as never, t);
  };

  useEffect(() => {
    const svg = svgRef.current!;
    const z = d3zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.05, 3])
      .on("zoom", (e) => viewRef.current?.setAttribute("transform", e.transform.toString()));
    zoomRef.current = z;
    select(svg).call(z).on("dblclick.zoom", null);
    return () => {
      select(svg).on(".zoom", null);
    };
  }, []);

  // refit whenever a new neighbourhood is laid out
  useEffect(() => fit(false), [width, height, graph.focus]); // eslint-disable-line react-hooks/exhaustive-deps

  const zoomBy = (f: number) => {
    if (svgRef.current && zoomRef.current) select(svgRef.current).transition().duration(200).call(zoomRef.current.scaleBy as never, f);
  };

  const touches = (e: Edge) => hover !== null && (e.advisor === hover || e.student === hover);

  return (
    <div className="graph">
      <svg ref={svgRef} role="group" aria-label="Advisors above, students below">
        <g ref={viewRef}>
          <g className="g-edges">
            {edges.map((e) => (
              <path key={`${e.advisor}-${e.student}`} d={e.d} className={touches(e) ? "edge edge-hot" : "edge"} />
            ))}
          </g>
          {placed.map(({ node, x, y }) => {
            const focus = node.id === graph.focus;
            const sub = [node.year ?? "—", node.school].filter(Boolean).join(" · ");
            return (
              <g
                key={node.id}
                className={`card ${focus ? "card-focus" : ""} ${node.depth < 0 ? "card-up" : node.depth > 0 ? "card-down" : ""}`}
                transform={`translate(${x - CARD_W / 2},${y - CARD_H / 2})`}
                role="link"
                tabIndex={0}
                aria-label={`${node.name}, ${sub}`}
                onClick={() => onOpen(node.id)}
                onKeyDown={(ev) => ev.key === "Enter" && onOpen(node.id)}
                onPointerEnter={() => setHover(node.id)}
                onPointerLeave={() => setHover(null)}
                onFocus={() => setHover(node.id)}
                onBlur={() => setHover(null)}
              >
                <title>{`${node.name}\n${sub}${node.country ? `\n${node.country}` : ""}\n${node.student_count} students on record`}</title>
                <rect width={CARD_W} height={CARD_H} rx={6} />
                <text x={10} y={19} className="card-name">
                  {clip(node.name, 25)}
                </text>
                <text x={10} y={35} className="card-sub">
                  {clip(String(sub), 31)}
                </text>
                {node.student_count > 0 && (
                  <text x={CARD_W - 8} y={19} className="card-count" textAnchor="end">
                    {node.student_count}
                  </text>
                )}
              </g>
            );
          })}
        </g>
      </svg>
      <div className="graph-zoom">
        <button type="button" onClick={() => zoomBy(1.4)} aria-label="Zoom in">
          +
        </button>
        <button type="button" onClick={() => zoomBy(1 / 1.4)} aria-label="Zoom out">
          −
        </button>
        <button type="button" onClick={() => fit(true)} aria-label="Fit to view">
          ⤢
        </button>
      </div>
    </div>
  );
}
