import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { useSearchParams } from "react-router";
import { useFlows, type Flows } from "../api/client";
import { arcsFor, FlowMap, type Arc } from "../map/FlowMap";
import { useWorld } from "../map/world";
import { shapeKey } from "../map/countries";
import { CountUp, rise } from "../motion";

const fmt = new Intl.NumberFormat();
const STEP_MS = 1800;

/** Where mathematics travelled: from the country of an advisor's degree to their student's. */
export default function FlowsPage() {
  const [params, setParams] = useSearchParams();
  // the decade and country live in the URL (?d=1930&c=Germany) so a view can be shared
  const decade = params.get("d") === null ? null : Number(params.get("d"));
  const country = params.get("c");
  const selected = country ? shapeKey(country) : null;
  const set = (next: { d?: number | null; c?: string | null }) => {
    const p = new URLSearchParams(params);
    for (const [k, v] of Object.entries(next)) v === null || v === undefined ? p.delete(k) : p.set(k, String(v));
    setParams(p, { replace: true });
  };

  const flows = useFlows(decade);
  const world = useWorld();
  const [highlighted, setHighlighted] = useState<string | null>(null);
  const [playing, setPlaying] = useState(false);
  useEffect(() => void (document.title = "Flows · Combi"), []);

  // every response carries the all-time decade counts, whatever range it was asked for
  const timeline = flows.data?.decades ?? [];

  // playing steps to the next decade once each has been on screen for STEP_MS
  useEffect(() => {
    if (!playing || !timeline.length) return;
    const t = setTimeout(() => {
      const next = timeline[timeline.findIndex((d) => d.decade === decade) + 1];
      if (next) set({ d: next.decade });
      else setPlaying(false);
    }, STEP_MS);
    return () => clearTimeout(t);
  }, [playing, decade, timeline]); // eslint-disable-line react-hooks/exhaustive-deps

  const placed = useMemo(() => (flows.data && world.data ? arcsFor(flows.data.flows, world.data) : null), [flows.data, world.data]);
  const period = decade === null ? "all time" : `students who graduated in the ${decade}s`;
  const names = useMemo(() => {
    const out = new Map<string, string>();
    for (const a of placed?.arcs ?? []) (out.set(a.from, a.fromName), out.set(a.to, a.toName));
    return out;
  }, [placed]);
  // the selection is a map region; the URL keeps an MGP country name for it
  const selectRegion = (key: string | null) => {
    const name = key && flows.data?.flows.find((f) => shapeKey(f.from) === key || shapeKey(f.to) === key);
    set({ c: key && name ? (shapeKey(name.from) === key ? name.from : name.to) : null });
  };

  const error = flows.error ?? world.error;
  return (
    <main className="mappage">
      <header>
        <h1 className="page-title">Where mathematics travelled</h1>
        <p className="muted">
          Each arc runs from the country where an advisor earned their degree to the country where their student earned theirs,
          thicker for more students. Pick a decade, or press play to watch it change; click a country for what came and went.
        </p>
      </header>
      {error && <p className="error">{error.message}</p>}
      {timeline.length > 0 && (
        <Timeline
          decades={timeline}
          decade={decade}
          playing={playing}
          onPick={(d) => (setPlaying(false), set({ d }))}
          onPlay={() => {
            if (playing) return setPlaying(false);
            // start over when play is pressed on the last decade
            if (decade === timeline.at(-1)!.decade || decade === null) set({ d: timeline[0]!.decade });
            setPlaying(true);
          }}
        />
      )}
      <div className="map-layout">
        <section className="map-card">
          {placed && world.data ? (
            <FlowMap
              shapes={world.data}
              arcs={placed.arcs}
              regions={placed.regions}
              selected={selected}
              highlighted={highlighted}
              drawKey={`${decade}-${selected}`}
              onSelect={selectRegion}
              onHighlight={setHighlighted}
              period={decade === null ? "all time" : `${decade}s`}
            />
          ) : (
            !error && <div className="map map-loading" aria-busy="true" />
          )}
        </section>
        <aside className="map-side">
          {placed &&
            (selected ? (
              <CountryFlows
                key={selected}
                name={names.get(selected) ?? country ?? ""}
                arcs={placed.arcs.filter((a) => a.from === selected || a.to === selected)}
                selected={selected}
                period={period}
                onHighlight={setHighlighted}
                onBack={() => set({ c: null })}
              />
            ) : (
              <TopFlows arcs={placed.arcs} period={period} onHighlight={setHighlighted} onSelect={selectRegion} />
            ))}
        </aside>
      </div>
    </main>
  );
}

function Timeline({ decades, decade, playing, onPick, onPlay }: { decades: Flows["decades"]; decade: number | null; playing: boolean; onPick: (d: number | null) => void; onPlay: () => void }) {
  const max = Math.max(...decades.map((d) => d.count));
  const every = Math.max(1, Math.ceil(decades.length / 8));
  return (
    <div className="timeline">
      <div className="timeline-buttons">
        <button type="button" className="btn" onClick={onPlay} aria-pressed={playing}>
          {playing ? "❚❚ Pause" : "▶ Play"}
        </button>
        <button type="button" className={`chip ${decade === null ? "is-on" : ""}`} onClick={() => onPick(null)}>
          All time
        </button>
      </div>
      <div className="timeline-track">
        <div className="decade-cols timeline-cols" role="radiogroup" aria-label="Decade of the students' degrees">
          {decades.map((d, i) => (
            <button
              key={d.decade}
              type="button"
              role="radio"
              aria-checked={decade === d.decade}
              aria-label={`${d.decade}s: ${d.count} cross-border students`}
              className={`decade ${decade === d.decade ? "is-on" : ""} ${decade !== null && decade !== d.decade ? "is-off" : ""}`}
              style={{ "--h": d.count / max, "--i": Math.min(i, 30) } as CSSProperties}
              onClick={() => onPick(decade === d.decade ? null : d.decade)}
            >
              <span className="decade-bar" />
              <span className="decade-tip">
                {d.decade}s · {fmt.format(d.count)}
              </span>
            </button>
          ))}
        </div>
        <div className="decade-axis mono" aria-hidden="true">
          {decades.map((d, i) => (
            <span key={d.decade}>{i % every === 0 || i === decades.length - 1 ? `${d.decade}s` : ""}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

function FlowRows({ arcs, max, label, onHighlight, onSelect }: { arcs: Arc[]; max: number; label: (a: Arc) => string; onHighlight: (id: string | null) => void; onSelect?: (a: Arc) => void }) {
  return (
    <ol className="bars">
      {arcs.map((a, i) => (
        <li key={a.id} className="rise" style={rise(i)} onPointerEnter={() => onHighlight(a.id)} onPointerLeave={() => onHighlight(null)}>
          <button type="button" onClick={() => onSelect?.(a)} onFocus={() => onHighlight(a.id)} onBlur={() => onHighlight(null)}>
            <span className="bar-name">{label(a)}</span>
            <span className="bar-value mono">{fmt.format(a.count)}</span>
            <span className="bar" style={{ "--w": a.count / max, "--i": Math.min(i, 12) } as CSSProperties} />
          </button>
        </li>
      ))}
    </ol>
  );
}

function TopFlows({ arcs, period, onHighlight, onSelect }: { arcs: Arc[]; period: string; onHighlight: (id: string | null) => void; onSelect: (key: string) => void }) {
  if (!arcs.length) return <p className="muted">No one crossed a border in this period, as far as the database knows.</p>;
  const total = arcs.reduce((n, a) => n + a.count, 0);
  return (
    <>
      <p className="label">
        <CountUp value={total} /> cross-border students · {period}
      </p>
      <FlowRows arcs={arcs.slice(0, 20)} max={arcs[0]!.count} label={(a) => `${a.fromName} → ${a.toName}`} onHighlight={onHighlight} onSelect={(a) => onSelect(a.to)} />
    </>
  );
}

function CountryFlows({ name, arcs, selected, period, onHighlight, onBack }: { name: string; arcs: Arc[]; selected: string; period: string; onHighlight: (id: string | null) => void; onBack: () => void }) {
  const out = arcs.filter((a) => a.from === selected);
  const into = arcs.filter((a) => a.to === selected);
  const max = Math.max(1, ...arcs.map((a) => a.count));
  const sum = (xs: Arc[]) => xs.reduce((n, a) => n + a.count, 0);
  return (
    <div className="country">
      <button type="button" className="back" onClick={onBack}>
        ← All flows
      </button>
      <h2 className="country-name">{name}</h2>
      <p className="muted small">{period}</p>
      <dl className="counts">
        <div>
          <dt>Students abroad</dt>
          <dd>
            <CountUp value={sum(out)} />
          </dd>
        </div>
        <div>
          <dt>From abroad</dt>
          <dd>
            <CountUp value={sum(into)} />
          </dd>
        </div>
      </dl>
      {out.length > 0 && (
        <>
          <h3 className="label">Their students graduated in</h3>
          <FlowRows arcs={out} max={max} label={(a) => a.toName} onHighlight={onHighlight} />
        </>
      )}
      {into.length > 0 && (
        <>
          <h3 className="label">Advisors came from</h3>
          <FlowRows arcs={into} max={max} label={(a) => a.fromName} onHighlight={onHighlight} />
        </>
      )}
    </div>
  );
}
