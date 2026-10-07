import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router";
import { ApiError, MAX_SCHOOL_PEOPLE, useSchool, type SchoolDetail } from "../api/client";
import { CountUp, rise } from "../motion";

export function SchoolPage() {
  const id = Number(useParams().id);
  const school = useSchool(id);
  const s = school.data;
  useEffect(() => {
    if (s) document.title = `${s.name} · Combi`;
  }, [s]);

  if (school.isError)
    return (
      <main className="page narrow">
        {school.error instanceof ApiError && school.error.status === 404 ? (
          <>
            <h1 className="page-title">No such school</h1>
            <p className="muted">There is no school with ID {id} in the database.</p>
          </>
        ) : (
          <p className="error">{school.error.message}</p>
        )}
      </main>
    );
  if (!s)
    return (
      <main className="page narrow">
        <p className="muted">Loading…</p>
      </main>
    );

  return (
    <main className="page narrow school">
      <p className="label">School</p>
      <h1 className="page-title">{s.name}</h1>
      {s.countries.length > 0 && (
        <p className="muted school-where">
          {s.countries.map((c, i) => (
            <span key={c.country}>
              {i > 0 && ", "}
              <Link to={`/map?c=${encodeURIComponent(c.country)}`}>{c.name}</Link>
            </span>
          ))}
        </p>
      )}
      <dl className="counts">
        <div>
          <dt>Mathematicians</dt>
          <dd>
            <CountUp value={s.mathematicians} />
          </dd>
        </div>
        <div>
          <dt>First degree</dt>
          <dd>{s.first_year ?? "—"}</dd>
        </div>
        <div>
          <dt>Latest degree</dt>
          <dd>{s.last_year ?? "—"}</dd>
        </div>
      </dl>
      {s.decades.length > 1 && <Decades decades={s.decades} />}
      <Graduates school={s} />
    </main>
  );
}

/** Degrees per decade as columns; every decade in the range gets a slot so gaps show as gaps. */
function Decades({ decades }: { decades: SchoolDetail["decades"] }) {
  const first = decades[0]!.decade;
  const last = decades.at(-1)!.decade;
  const counts = new Map(decades.map((d) => [d.decade, d.count]));
  const slots = Array.from({ length: (last - first) / 10 + 1 }, (_, i) => first + i * 10);
  const max = Math.max(...decades.map((d) => d.count));
  // label about six decades along the axis, always the first and the last
  const every = Math.max(1, Math.ceil(slots.length / 6));
  return (
    <figure className="decades">
      <figcaption className="label">Degrees by decade · most {max} in a decade</figcaption>
      <div className="decade-cols" role="list">
        {slots.map((d, i) => {
          const n = counts.get(d) ?? 0;
          return (
            <div key={d} className="decade" role="listitem" aria-label={`${d}s: ${n}`} style={{ "--h": n / max, "--i": Math.min(i, 30) } as React.CSSProperties}>
              <span className="decade-bar" />
              <span className="decade-tip">
                {d}s · {n}
              </span>
            </div>
          );
        })}
      </div>
      <div className="decade-axis mono" aria-hidden="true">
        {slots.map((d, i) => (
          <span key={d}>{i % every === 0 || i === slots.length - 1 ? `${d}s` : ""}</span>
        ))}
      </div>
    </figure>
  );
}

function Graduates({ school }: { school: SchoolDetail }) {
  const [filter, setFilter] = useState("");
  const shown = useMemo(() => {
    const f = filter.trim().toLowerCase().normalize("NFD").replace(/\p{M}/gu, "");
    if (!f) return school.people;
    return school.people.filter((p) => p.name.toLowerCase().normalize("NFD").replace(/\p{M}/gu, "").includes(f) || String(p.year) === f);
  }, [filter, school.people]);

  return (
    <section className="graduates">
      <div className="graduates-head">
        <h2 className="label">Graduates, newest first</h2>
        {school.people.length > 8 && (
          <input
            type="search"
            className="filter"
            placeholder="Filter by name or year"
            aria-label="Filter graduates"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        )}
      </div>
      {school.people.length === MAX_SCHOOL_PEOPLE && (
        <p className="muted small">Showing the newest {MAX_SCHOOL_PEOPLE} of {school.mathematicians}.</p>
      )}
      <table className="results">
        <thead>
          <tr>
            <th>Name</th>
            <th className="num">Ph.D.</th>
            <th className="num">Students</th>
          </tr>
        </thead>
        <tbody>
          {shown.slice(0, 500).map((p, i) => (
            <tr key={p.id} className="rise" style={rise(i)}>
              <td>
                <Link to={`/m/${p.id}`}>{p.name}</Link>
              </td>
              <td className="num mono">{p.year ?? "—"}</td>
              <td className="num mono">{p.student_count || ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {shown.length > 500 && <p className="muted small">{shown.length - 500} more; filter to narrow the list.</p>}
      {!shown.length && <p className="muted small">No one matches “{filter}”.</p>}
    </section>
  );
}
