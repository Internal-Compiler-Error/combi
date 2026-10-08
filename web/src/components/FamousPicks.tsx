import { useMemo, useState } from "react";
import { Link } from "react-router";
import { CLASSICS, COMPUTING, FOUNDATIONS, MODERN, PRIZEWINNERS, type Famous } from "../../shared/famous";
import { degreeLine, useNotable, usePeople, useRandomPeople, type Person } from "../api/client";
import { rise } from "../motion";

/** how many people a tab shows at a time */
const SHOWN = 6;

type Tab = { key: string; label: string } & ({ list: Famous[] } | { live: "random" | "students" });

const TABS: Tab[] = [
  { key: "classics", label: "Classics", list: CLASSICS },
  { key: "modern", label: "Modern", list: MODERN },
  { key: "computing", label: "Computer science", list: COMPUTING },
  { key: "foundations", label: "Foundations", list: FOUNDATIONS },
  { key: "prizes", label: "Prizewinners", list: PRIZEWINNERS },
  { key: "random", label: "Random", live: "random" },
  { key: "students", label: "Most students", live: "students" },
];

const STORAGE_KEY = "combi.famousTab";

function remembered(): string {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved && TABS.some((t) => t.key === saved)) return saved;
  } catch {
    // storage can be unavailable (private windows); start on the first tab
  }
  return TABS[0]!.key;
}

function sample<T>(xs: T[], n: number): T[] {
  const pool = [...xs];
  for (let i = pool.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [pool[i], pool[j]] = [pool[j]!, pool[i]!];
  }
  return pool.slice(0, n);
}

type Card = { id: number; name: string; known: string | null; person: Person | undefined };

/**
 * One group of famous mathematicians at a time, behind a row of tabs so the home page stays short.
 * Every visit, tab change and Shuffle draws a fresh few.
 */
export function FamousPicks() {
  const [tabKey, setTabKey] = useState(remembered);
  const [draw, setDraw] = useState(0);
  const tab = TABS.find((t) => t.key === tabKey)!;

  const picked = useMemo(() => ("list" in tab ? sample(tab.list, SHOWN) : []), [tab, draw]); // eslint-disable-line react-hooks/exhaustive-deps
  const curated = usePeople(picked.map((f) => f.id));
  const random = useRandomPeople(SHOWN, draw, "live" in tab && tab.live === "random");
  const notable = useNotable();
  const mostStudents = useMemo(() => sample(notable.data ?? [], SHOWN), [notable.data, draw]);

  const cards: Card[] =
    "list" in tab
      ? picked.map((f) => ({ ...f, person: curated.data?.find((p) => p.id === f.id) }))
      : (tab.live === "random" ? (random.data ?? []) : mostStudents).map((p) => ({ id: p.id, name: p.name, known: null, person: p }));

  const choose = (key: string) => {
    setTabKey(key);
    setDraw((d) => d + 1);
    try {
      localStorage.setItem(STORAGE_KEY, key);
    } catch {
      // not remembering the tab is fine
    }
  };

  return (
    <section className="famous">
      <div className="famous-head">
        <h2 className="label">Famous mathematicians</h2>
        <button type="button" className="chip" onClick={() => setDraw((d) => d + 1)}>
          ↻ Shuffle
        </button>
      </div>
      <div className="famous-tabs" role="tablist" aria-label="Groups of mathematicians">
        {TABS.map((t) => (
          <button key={t.key} type="button" role="tab" aria-selected={t.key === tabKey} className={`chip ${t.key === tabKey ? "is-on" : ""}`} onClick={() => choose(t.key)}>
            {t.label}
          </button>
        ))}
      </div>
      {/* keyed by tab and draw so each new set is dealt in */}
      <ul className="notable-grid" role="tabpanel" key={`${tabKey}-${draw}`}>
        {cards.map((c, i) => (
          <li key={c.id} className="rise" style={rise(i)}>
            <Link to={`/m/${c.id}`} className="notable-card">
              <span className="notable-name">{c.name}</span>
              {c.known && <span className="famous-known">{c.known}</span>}
              <span className="muted small">{c.person ? degreeLine(c.person) : "Not crawled yet"}</span>
              {c.person && c.person.student_count > 0 && (
                <span className="notable-count">
                  <b>{c.person.student_count}</b> students
                </span>
              )}
            </Link>
          </li>
        ))}
      </ul>
    </section>
  );
}
