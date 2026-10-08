import { useMemo, useState } from "react";
import { Link } from "react-router";
import { CLASSICS, COMPUTING, MODERN, type Famous } from "../../shared/famous";
import { degreeLine, usePeople, type Person } from "../api/client";
import { rise } from "../motion";

/** how many of each list the home page shows at a time */
const SHOWN = 6;

function sample<T>(xs: T[], n: number): T[] {
  const pool = [...xs];
  for (let i = pool.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [pool[i], pool[j]] = [pool[j]!, pool[i]!];
  }
  return pool.slice(0, n);
}

/** A random few from each list, freshly drawn on each visit and on Shuffle. */
export function FamousPicks() {
  const [draw, setDraw] = useState(0);
  const picks = useMemo(
    () => ({ classics: sample(CLASSICS, SHOWN), modern: sample(MODERN, SHOWN), computing: sample(COMPUTING, SHOWN) }),
    [draw], // eslint-disable-line react-hooks/exhaustive-deps
  );
  const people = usePeople([...picks.classics, ...picks.modern, ...picks.computing].map((f) => f.id));
  const known = new Map((people.data ?? []).map((p) => [p.id, p]));

  return (
    <section className="famous">
      <div className="famous-head">
        <h2 className="label">Famous mathematicians</h2>
        <button type="button" className="chip" onClick={() => setDraw((d) => d + 1)}>
          ↻ Shuffle
        </button>
      </div>
      <Group title="Classics" picks={picks.classics} known={known} draw={draw} />
      <Group title="Modern" picks={picks.modern} known={known} draw={draw} />
      <Group title="Computer science and nearby" picks={picks.computing} known={known} draw={draw} />
    </section>
  );
}

function Group({ title, picks, known, draw }: { title: string; picks: Famous[]; known: Map<number, Person>; draw: number }) {
  return (
    <div className="famous-group">
      <h3 className="famous-title">{title}</h3>
      {/* keyed by draw so a shuffle deals the cards in again */}
      <ul className="notable-grid" key={draw}>
        {picks.map((f, i) => {
          const p = known.get(f.id);
          return (
            <li key={f.id} className="rise" style={rise(i)}>
              <Link to={`/m/${f.id}`} className="notable-card">
                <span className="notable-name">{f.name}</span>
                <span className="famous-known">{f.known}</span>
                <span className="muted small">{p ? degreeLine(p) : "Not crawled yet"}</span>
                {p && p.student_count > 0 && (
                  <span className="notable-count">
                    <b>{p.student_count}</b> students
                  </span>
                )}
              </Link>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
