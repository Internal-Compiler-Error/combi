import { Link } from "react-router";
import type { Person } from "../api/client";

/** A compact list of people, each linking to their own page. */
export function PersonList({ people, empty }: { people: Person[]; empty: string }) {
  if (!people.length) return <p className="muted small">{empty}</p>;
  return (
    <ul className="people">
      {people.map((p) => (
        <li key={p.id}>
          <Link to={`/m/${p.id}`} className="people-name">
            {p.name}
          </Link>
          <span className="people-meta">
            {p.student_count > 0 && <span className="people-count" title={`${p.student_count} students on record`}>{p.student_count}</span>}
            <span className="mono">{p.year ?? "—"}</span>
          </span>
        </li>
      ))}
    </ul>
  );
}
