import { useEffect } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { Link } from "react-router";
import { ApiError, mgpUrl, useGraph, usePerson, type Graph } from "../api/client";
import { DegreeLine } from "../components/DegreeLine";
import { RadialGraph } from "../components/RadialGraph";
import { PersonList } from "../components/PersonList";
import { CrawlButton } from "../components/CrawlButton";
import { CountUp } from "../motion";

const MAX_DEPTH = 6;

function Stepper({ label, value, onChange }: { label: string; value: number; onChange: (v: number) => void }) {
  return (
    <div className="stepper" role="group" aria-label={label}>
      <span className="stepper-label">{label}</span>
      <button type="button" onClick={() => onChange(value - 1)} disabled={value <= 0} aria-label={`Fewer ${label.toLowerCase()}`}>
        −
      </button>
      <output className="mono" aria-live="polite">
        {value}
      </output>
      <button type="button" onClick={() => onChange(value + 1)} disabled={value >= MAX_DEPTH} aria-label={`More ${label.toLowerCase()}`}>
        +
      </button>
    </div>
  );
}

/** Advisors above the focus, nearest generation first; the radial graph only shows the generations below. */
function Ancestry({ graph, linkTo }: { graph: Graph; linkTo: (id: number) => string }) {
  const rows = new Map<number, Graph["nodes"]>();
  for (const n of graph.nodes) if (n.depth < 0) rows.set(n.depth, [...(rows.get(n.depth) ?? []), n]);
  if (!rows.size) return null;
  const depths = [...rows.keys()].sort((a, b) => b - a);
  return (
    <ol className="ancestry" aria-label="Advisor generations">
      {depths.map((d) => (
        <li key={d}>
          <span className="ancestry-gen mono">{d === -1 ? "Advisors" : `${-d} up`}</span>
          <span className="ancestry-people">
            {rows.get(d)!.map((n) => (
              <Link key={n.id} to={linkTo(n.id)}>
                {n.name}
                {n.year && <span className="muted mono small"> {n.year}</span>}
              </Link>
            ))}
          </span>
        </li>
      ))}
    </ol>
  );
}

const depthParam = (raw: string | null, fallback: number) => {
  const n = Number(raw ?? fallback);
  return Number.isInteger(n) ? Math.min(MAX_DEPTH, Math.max(0, n)) : fallback;
};

export function PersonPage() {
  const id = Number(useParams().id);
  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  // generations shown live in the URL so a view can be shared and survives reloads
  const up = depthParam(params.get("up"), 2);
  const down = depthParam(params.get("down"), 3);

  const person = usePerson(id);
  const graph = useGraph(id, { up, down });
  const p = person.data;

  useEffect(() => {
    if (p) document.title = `${p.name} · Combi`;
  }, [p]);

  const setDepth = (key: "up" | "down", v: number) => {
    const next = new URLSearchParams(params);
    next.set(key, String(v));
    setParams(next, { replace: true });
  };

  // keep the chosen depths when moving to another person
  const linkTo = (to: number) => `/m/${to}?${new URLSearchParams({ up: String(up), down: String(down) })}`;
  const open = (to: number) => navigate(linkTo(to));

  if (!Number.isInteger(id)) return <Missing />;
  if (person.isError) return person.error instanceof ApiError && person.error.status === 404 ? <Missing id={id} /> : <main className="page"><p className="error">{person.error.message}</p></main>;
  if (!p) return <main className="page"><p className="muted">Loading…</p></main>;

  const g = graph.data;
  const alone = g && !g.nodes.some((n) => n.depth > 0);

  return (
    <main className="person">
      <section className="person-head">
        <p className="label">MGP ID {p.id}</p>
        <h1>{p.name}</h1>
        <p className="person-degree">
          <DegreeLine p={p} />
          {p.country && <span className="muted"> · {p.country}</span>}
        </p>
        {p.dissertation ? <blockquote className="dissertation">{p.dissertation}</blockquote> : <p className="muted small">No dissertation title on record.</p>}
        <dl className="counts">
          <div>
            <dt>Advisors</dt>
            <dd>
              <CountUp value={p.advisors.length} />
            </dd>
          </div>
          <div>
            <dt>Students</dt>
            <dd>
              <CountUp value={p.student_count} />
            </dd>
          </div>
          <div>
            <dt>Descendants</dt>
            <dd>
              <CountUp value={p.descendant_count} />
            </dd>
          </div>
        </dl>
        {!p.last_crawled && (
          <p className="notice small">
            {p.name}’s own page hasn’t been crawled yet; this is only what their advisors’ and students’ pages say.
          </p>
        )}
        <CrawlButton id={p.id} lastCrawled={p.last_crawled} label={p.last_crawled ? "Crawl again" : "Crawl their page"} />
        <a className="ext" href={mgpUrl(p.id)} target="_blank" rel="noreferrer">
          Open on Mathematics Genealogy Project ↗
        </a>
      </section>

      <section className="person-graph">
        <div className="graph-toolbar">
          <Stepper label="Advisor generations" value={up} onChange={(v) => setDepth("up", v)} />
          <Stepper label="Student generations" value={down} onChange={(v) => setDepth("down", v)} />
          <span className="muted small graph-hint">{graph.isFetching ? "Loading…" : g ? `${g.nodes.length} people · click anyone to recentre` : ""}</span>
        </div>
        {g && <Ancestry graph={g} linkTo={linkTo} />}
        {g?.truncated && <p className="notice small">This view is capped at {g.nodes.length} people; the farthest generations are cut off. Lower the generations to see a complete picture.</p>}
        {graph.isError && <p className="error">{graph.error.message}</p>}
        {alone ? (
          <div className="graph graph-empty">
            <p className="muted">
              {down === 0 ? "Add a student generation to see the tree." : `No students of ${p.name} are in the database yet.`}
            </p>
          </div>
        ) : (
          g && <RadialGraph graph={g} onOpen={open} />
        )}
      </section>

      <section className="person-lists">
        <div>
          <h2 className="label">Advisors</h2>
          <PersonList people={p.advisors} empty="No advisors on record." />
        </div>
        <div>
          <h2 className="label">Students</h2>
          <PersonList people={p.students} empty="No students on record." />
        </div>
      </section>
    </main>
  );
}

function Missing({ id }: { id?: number }) {
  return (
    <main className="page narrow">
      <h1 className="page-title">Not in the database</h1>
      <p className="muted">
        {id !== undefined ? (
          <>
            Nobody with MGP ID {id} has been crawled yet. You can still{" "}
            <a href={mgpUrl(id)} target="_blank" rel="noreferrer">
              look them up on the Mathematics Genealogy Project ↗
            </a>
            .
          </>
        ) : (
          "That isn’t a valid ID."
        )}
      </p>
      {id !== undefined && <CrawlButton id={id} lastCrawled={null} label="Crawl this page" />}
    </main>
  );
}
