import { useEffect } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router";
import { ApiError, degreeLine, mgpUrl, useGraph, usePerson } from "../api/client";
import { LineageGraph } from "../components/LineageGraph";
import { PersonList } from "../components/PersonList";

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
  const down = depthParam(params.get("down"), 2);

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
  const open = (to: number) => navigate(`/m/${to}?${new URLSearchParams({ up: String(up), down: String(down) })}`);

  if (!Number.isInteger(id)) return <Missing />;
  if (person.isError) return person.error instanceof ApiError && person.error.status === 404 ? <Missing id={id} /> : <main className="page"><p className="error">{person.error.message}</p></main>;
  if (!p) return <main className="page"><p className="muted">Loading…</p></main>;

  const g = graph.data;
  const alone = g && g.nodes.length <= 1;

  return (
    <main className="person">
      <section className="person-head">
        <p className="label">MGP ID {p.id}</p>
        <h1>{p.name}</h1>
        <p className="person-degree">
          {degreeLine(p)}
          {p.country && <span className="muted"> · {p.country}</span>}
        </p>
        {p.dissertation ? <blockquote className="dissertation">{p.dissertation}</blockquote> : <p className="muted small">No dissertation title on record.</p>}
        <dl className="counts">
          <div>
            <dt>Advisors</dt>
            <dd>{p.advisors.length}</dd>
          </div>
          <div>
            <dt>Students</dt>
            <dd>{p.student_count}</dd>
          </div>
          <div>
            <dt>Descendants</dt>
            <dd>{p.descendant_count}</dd>
          </div>
        </dl>
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
        {g?.truncated && <p className="notice small">This view is capped at {g.nodes.length} people; the farthest generations are cut off. Lower the generations to see a complete picture.</p>}
        {graph.isError && <p className="error">{graph.error.message}</p>}
        {alone ? (
          <div className="graph graph-empty">
            <p className="muted">No advisors or students of {p.name} are in the database yet.</p>
          </div>
        ) : (
          g && <LineageGraph graph={g} onOpen={open} />
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
    </main>
  );
}
