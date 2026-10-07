import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import type { Region, Shape } from "./world";
import { fitWorld, MAP_WIDTH, shadeOf } from "./world";
const fmt = new Intl.NumberFormat();

type Props = {
  shapes: Shape[];
  regions: Map<string, Region>;
  thresholds: number[];
  selected: string | null;
  onSelect: (region: Region | null) => void;
};

/** Mathematicians per country as a choropleth; hover for counts, click a shaded country for its schools. */
export function WorldMap({ shapes, regions, thresholds, selected, onSelect }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<{ key: string; x: number; y: number } | null>(null);
  // first paint is unshaded; the shades then wash in, largest countries first
  const [ready, setReady] = useState(false);
  useEffect(() => {
    let inner = 0;
    const outer = requestAnimationFrame(() => (inner = requestAnimationFrame(() => setReady(true))));
    return () => (cancelAnimationFrame(outer), cancelAnimationFrame(inner));
  }, []);

  const { path, projection: project, height } = useMemo(() => fitWorld(shapes), [shapes]);

  const byShape = useMemo(() => new Map([...regions.values()].filter((r) => r.shape).map((r) => [r.shape!, r])), [regions]);
  const dots = useMemo(() => [...regions.values()].filter((r) => r.dot), [regions]);
  const rank = useMemo(() => new Map([...regions.values()].sort((a, b) => b.mathematicians - a.mathematicians).map((r, i) => [r.key, i])), [regions]);

  const style = (r: Region): CSSProperties =>
    ({ "--shade": `var(--seq-${shadeOf(r.mathematicians, thresholds)})`, "--i": Math.min(rank.get(r.key) ?? 0, 40) }) as CSSProperties;

  const track = (key: string) => (e: React.PointerEvent) => {
    const box = wrapRef.current!.getBoundingClientRect();
    setHover({ key, x: e.clientX - box.left, y: e.clientY - box.top });
  };
  const hovered = hover && (regions.get(hover.key) ?? shapes.find((s) => s.properties.name === hover.key));
  const outline = (key: string | null) => {
    const r = key ? regions.get(key) : undefined;
    return r?.shape ? path(r.shape) ?? undefined : undefined;
  };

  return (
    <div className="map" ref={wrapRef}>
      <svg
        viewBox={`0 0 ${MAP_WIDTH} ${height}`}
        className={ready ? "map-ready" : undefined}
        role="img"
        aria-label="World map shaded by how many mathematicians graduated from schools in each country; the list beside it has the same numbers"
        onPointerLeave={() => setHover(null)}
      >
        <g>
          {shapes.map((s) => {
            const r = byShape.get(s);
            return (
              <path
                key={s.properties.name}
                d={path(s) ?? undefined}
                className={r ? "map-country has-data" : "map-country"}
                style={r ? style(r) : undefined}
                onPointerMove={track(r?.key ?? s.properties.name)}
                onClick={() => r && onSelect(r.key === selected ? null : r)}
              />
            );
          })}
        </g>
        {/* the hovered and selected countries are outlined on top, so their borders aren't hidden by neighbours */}
        <path className="map-outline map-outline-selected" d={outline(selected)} />
        <path className="map-outline" d={hover && hover.key !== selected ? outline(hover.key) : undefined} />
        <g>
          {dots.map((r) => {
            const [x, y] = project(r.dot!) ?? [0, 0];
            return (
              <circle
                key={r.key}
                cx={x}
                cy={y}
                r={r.key === selected || r.key === hover?.key ? 6 : 4.5}
                className="map-dot"
                style={style(r)}
                onPointerMove={track(r.key)}
                onClick={() => onSelect(r.key === selected ? null : r)}
              />
            );
          })}
        </g>
      </svg>

      {hover && hovered && (
        <div
          className="tip"
          // flips to the cursor's left near the right edge rather than sliding over the country itself
          style={hover.x + 250 > (wrapRef.current?.clientWidth ?? 0) ? { right: (wrapRef.current?.clientWidth ?? 0) - hover.x + 14, top: hover.y + 14 } : { left: hover.x + 14, top: hover.y + 14 }}
        >
          {"parts" in hovered ? (
            <>
              <b>{hovered.name}</b>
              {fmt.format(hovered.mathematicians)} mathematician{hovered.mathematicians === 1 ? "" : "s"} · {hovered.schools} school
              {hovered.schools === 1 ? "" : "s"}
              {hovered.parts.length > 1 && (
                <>
                  <br />
                  {hovered.parts.map((p) => `${p.name} ${fmt.format(p.mathematicians)}`).join(" · ")}
                </>
              )}
            </>
          ) : (
            <>
              <b>{hovered.properties.name}</b>
              No one on record yet
            </>
          )}
        </div>
      )}

      <Legend thresholds={thresholds} />
    </div>
  );
}

function Legend({ thresholds }: { thresholds: number[] }) {
  const bounds = [1, ...thresholds];
  return (
    <div className="map-legend" aria-label="Mathematicians per country">
      <span className="map-legend-title">Mathematicians</span>
      {bounds.map((lo, i) => {
        const hi = bounds[i + 1];
        return (
          <span key={lo} className="map-legend-item">
            <i style={{ background: `var(--seq-${i + 1})` }} />
            {hi === undefined ? `${fmt.format(lo)}+` : hi - 1 === lo ? fmt.format(lo) : `${fmt.format(lo)}–${fmt.format(hi - 1)}`}
          </span>
        );
      })}
      <span className="map-legend-item">
        <i className="map-legend-empty" />
        None yet
      </span>
    </div>
  );
}
