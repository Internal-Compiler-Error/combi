import { useEffect } from "react";
import { Link } from "react-router";
import { degreeLine, useNotable, useStats } from "../api/client";
import { SearchBox } from "../components/SearchBox";
import { FamousPicks } from "../components/FamousPicks";
import { CountUp, rise } from "../motion";

export function HomePage() {
  const stats = useStats();
  const notable = useNotable();
  useEffect(() => void (document.title = "Combi · Mathematics genealogy"), []);

  const s = stats.data;
  return (
    <main className="home">
      <section className="hero">
        <h1>Find any mathematician’s advisors and students</h1>
        <p className="lede">
          Search the crawled Mathematics Genealogy Project data by name or ID, then follow the lineage up to advisors or down to
          students.
        </p>
        <SearchBox large autoFocus />
        {s && (
          <dl className="facts">
            <div className="rise" style={rise(0)}>
              <dt>Mathematicians</dt>
              <dd>
                <CountUp value={s.mathematicians} />
              </dd>
            </div>
            <div className="rise" style={rise(1)}>
              <dt>Advisor links</dt>
              <dd>
                <CountUp value={s.relations} />
              </dd>
            </div>
            <div className="rise" style={rise(2)}>
              <dt>Degrees</dt>
              <dd>{s.first_year && s.last_year ? `${s.first_year}–${s.last_year}` : "—"}</dd>
            </div>
            <div className="rise" style={rise(3)}>
              <dt>
                <Link to="/map">Countries</Link>
              </dt>
              <dd>
                <CountUp value={s.countries} />
              </dd>
            </div>
            {s.last_scraped && (
              <div className="rise" style={rise(4)}>
                <dt>Last crawled</dt>
                <dd>{new Date(s.last_scraped).toLocaleDateString(undefined, { dateStyle: "medium" })}</dd>
              </div>
            )}
          </dl>
        )}
        {stats.isError && <p className="error">Couldn’t reach the API: {stats.error.message}</p>}
      </section>

      <FamousPicks />

      {notable.data && notable.data.length > 0 && (
        <section className="notable">
          <h2 className="label">Most students on record</h2>
          <ul className="notable-grid">
            {notable.data.map((p, i) => (
              <li key={p.id} className="rise" style={rise(i)}>
                <Link to={`/m/${p.id}`} className="notable-card">
                  <span className="notable-name">{p.name}</span>
                  <span className="muted small">{degreeLine(p)}</span>
                  <span className="notable-count">
                    <b>{p.student_count}</b> students
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </section>
      )}
      {s && s.mathematicians === 0 && (
        <p className="muted">
          The database is empty. Search for anyone above to find them on the Mathematics Genealogy Project and crawl
          them, or start with <Link to="/m/10416">Donald Knuth</Link>.
        </p>
      )}
    </main>
  );
}
