import { useEffect, useState, type CSSProperties } from "react";
import { Link, useNavigate, useSearchParams } from "react-router";
import { ancestorRole, kinship } from "../../shared/names";
import { degreeLine, usePerson, useRelation, useSearch, type Person, type Relation } from "../api/client";
import { useDebounced } from "../components/SearchBox";

const idParam = (raw: string | null) => (raw && /^\d+$/.test(raw) ? Number(raw) : null);

/** Pick two mathematicians and see how their academic families meet. */
export function RelatePage() {
  const [params, setParams] = useSearchParams();
  const a = idParam(params.get("a"));
  const b = idParam(params.get("b"));
  const pick = (key: "a" | "b") => (id: number | null) => {
    const p = new URLSearchParams(params);
    if (id === null) p.delete(key);
    else p.set(key, String(id));
    setParams(p, { replace: true });
  };
  const relation = useRelation(a, b);
  useEffect(() => void (document.title = "Relate · Combi"), []);

  return (
    <main className="page narrow relate">
      <h1 className="page-title">How are they related?</h1>
      <p className="muted">
        Pick two mathematicians to find their nearest shared academic ancestor: the advisor whose students’ students eventually led to
        both of them.
      </p>
      <div className="pickers">
        <PersonPicker id={a} label="First mathematician" onPick={pick("a")} />
        <span className="pickers-and" aria-hidden="true">
          and
        </span>
        <PersonPicker id={b} label="Second mathematician" onPick={pick("b")} />
      </div>
      {relation.isPending && a !== null && b !== null && <p className="muted">Tracing their advisors…</p>}
      {relation.isError && <p className="error">{relation.error.message}</p>}
      {relation.data && <Result relation={relation.data} key={`${a}-${b}`} />}
    </main>
  );
}

function PersonPicker({ id, label, onPick }: { id: number | null; label: string; onPick: (id: number | null) => void }) {
  const person = usePerson(id ?? 0, id !== null);
  const [text, setText] = useState("");
  const q = useDebounced(text.trim(), 150);
  const { data: hits = [] } = useSearch(q, 6);

  if (id !== null && person.data)
    return (
      <div className="picker picker-chosen">
        <span className="label">{label}</span>
        <Link to={`/m/${id}`} className="picker-name">
          {person.data.name}
        </Link>
        <span className="muted small">{degreeLine(person.data)}</span>
        <button type="button" className="back" onClick={() => onPick(null)}>
          Change
        </button>
      </div>
    );
  return (
    <div className="picker search">
      <span className="label">{label}</span>
      <input type="search" value={text} placeholder="Name or MGP ID" aria-label={label} onChange={(e) => setText(e.target.value)} />
      {q && (
        <ul className="suggestions" role="listbox">
          {hits.map((p) => (
            <li key={p.id} role="option" aria-selected={false}>
              <button type="button" onClick={() => (setText(""), onPick(p.id))}>
                <span className="s-name">{p.name}</span>
                <span className="s-meta">{degreeLine(p)}</span>
              </button>
            </li>
          ))}
          {!hits.length && <li className="s-empty">No one in the database matches “{q}”.</li>}
        </ul>
      )}
    </div>
  );
}

function Result({ relation: r }: { relation: Relation }) {
  if (!r.ancestor)
    return (
      <section className="relate-result">
        <p className="relate-headline">
          No shared ancestor yet for <Link to={`/m/${r.a.id}`}>{r.a.name}</Link> and <Link to={`/m/${r.b.id}`}>{r.b.name}</Link>.
        </p>
        <p className="muted">
          Their families may still meet further up: the database only knows the advisors whose pages have been crawled. Crawling the
          oldest advisors on their pages takes their lines further back.
        </p>
      </section>
    );

  const depthA = r.pathA.length - 1;
  const depthB = r.pathB.length - 1;
  const lineal = depthA === 0 || depthB === 0;
  const link = (p: Person) => <Link to={`/m/${p.id}`}>{p.name}</Link>;
  // one descends from the other: say which way round
  const [elder, younger] = depthA === 0 ? [r.a, r.b] : [r.b, r.a];
  return (
    <section className="relate-result">
      {r.a.id === r.b.id ? (
        <p className="relate-headline">Pick two different people to compare.</p>
      ) : lineal ? (
        <p className="relate-headline">
          {link(elder)} is {link(younger)}’s {ancestorRole(Math.max(depthA, depthB))}.
        </p>
      ) : (
        <p className="relate-headline">
          {link(r.a)} and {link(r.b)} are {kinship(depthA, depthB)}, through {link(r.ancestor)}.
        </p>
      )}
      <p className="muted small">
        {depthA === 0 || depthB === 0
          ? `${Math.max(depthA, depthB)} generation${Math.max(depthA, depthB) === 1 ? "" : "s"} apart.`
          : `${depthA} generation${depthA === 1 ? "" : "s"} down to ${r.a.name}, ${depthB} down to ${r.b.name}.`}
      </p>
      <Lineage relation={r} />
    </section>
  );
}

const W = 860;
const ROW = 84;
const TOP = 52;
const SPREAD = 150;

/** The ancestor at the top, forking into the two lines of descent (one line when one descends from the other). */
function Lineage({ relation: r }: { relation: Relation }) {
  const navigate = useNavigate();
  const fork = r.pathA.length > 1 && r.pathB.length > 1;
  const single = r.pathA.length > 1 ? r.pathA : r.pathB;
  type Node = { p: Person; x: number; y: number; gen: number; side: "left" | "right" | "top"; end: "a" | "b" | null };
  const nodes: Node[] = [{ p: r.ancestor!, x: W / 2, y: TOP, gen: 0, side: "top", end: r.ancestor!.id === r.a.id ? "a" : r.ancestor!.id === r.b.id ? "b" : null }];
  const links: { d: string; gen: number }[] = [];
  const chain = (path: Person[], x: number, side: "left" | "right", end: "a" | "b") => {
    path.slice(1).forEach((p, i) => {
      const prev = nodes.find((n) => n.p.id === path[i]!.id && (n.side === side || n.side === "top"))!;
      const node = { p, x, y: TOP + (i + 1) * ROW, gen: i + 1, side, end: i + 1 === path.length - 1 ? end : null };
      nodes.push(node);
      const mid = (prev.y + node.y) / 2;
      links.push({ d: `M${prev.x},${prev.y}C${prev.x},${mid} ${node.x},${mid} ${node.x},${node.y}`, gen: i + 1 });
    });
  };
  if (fork) {
    chain(r.pathA, W / 2 - SPREAD, "left", "a");
    chain(r.pathB, W / 2 + SPREAD, "right", "b");
  } else {
    chain(single, W / 2, "right", r.pathA.length > 1 ? "a" : "b");
  }
  const height = TOP + (Math.max(r.pathA.length, r.pathB.length) - 1) * ROW + 40;
  const gen = (g: number) => ({ "--gen": g }) as CSSProperties;

  return (
    <svg className="lineage" viewBox={`0 0 ${W} ${height}`} role="img" aria-label={`Lines of descent from ${r.ancestor!.name}`}>
      {links.map((l, i) => (
        <path key={i} d={l.d} pathLength={1} className="lineage-link" style={gen(l.gen)} />
      ))}
      {nodes.map((n) => {
        const labelX = n.side === "left" ? n.x - 16 : n.side === "right" ? n.x + 16 : n.x;
        const anchor = n.side === "left" ? "end" : n.side === "right" ? "start" : "middle";
        const labelY = n.side === "top" ? n.y - 30 : n.y - 4;
        return (
          <g
            key={`${n.side}-${n.p.id}`}
            className={`lineage-node ${n.end ? `is-${n.end}` : ""} ${n.side === "top" ? "is-ancestor" : ""}`}
            style={gen(n.gen)}
            role="link"
            tabIndex={0}
            aria-label={`${n.p.name}, ${degreeLine(n.p)}`}
            onClick={() => navigate(`/m/${n.p.id}`)}
            onKeyDown={(e) => e.key === "Enter" && navigate(`/m/${n.p.id}`)}
          >
            <circle cx={n.x} cy={n.y} r={n.end || n.side === "top" ? 8 : 5} />
            <text x={labelX} y={labelY} textAnchor={anchor} className="lineage-name">
              {n.p.name}
            </text>
            <text x={labelX} y={labelY + 17} textAnchor={anchor} className="lineage-meta">
              {[n.p.year, n.p.school].filter(Boolean).join(" · ") || "Year unknown"}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
