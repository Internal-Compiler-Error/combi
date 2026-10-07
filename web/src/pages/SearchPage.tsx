import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useMgpSearch, useSearch, type Person } from "../api/client";
import { CrawlButton } from "../components/CrawlButton";

export function SearchPage() {
  const [params] = useSearchParams();
  const q = params.get("q") ?? "";
  const { data, isPending, isError, error } = useSearch(q, 100);
  useEffect(() => void (document.title = `“${q}” · Combi`), [q]);

  const id = /^\d+$/.test(q.trim()) ? Number(q.trim()) : null;

  return (
    <main className="page narrow">
      <h1 className="page-title">Results for “{q}”</h1>
      {isPending && q && <p className="muted">Searching…</p>}
      {isError && <p className="error">{error.message}</p>}
      {data && (
        <>
          <p className="muted small">{data.length === 100 ? "Showing the first 100 matches." : `${data.length} match${data.length === 1 ? "" : "es"}.`}</p>
          {data.length > 0 && <Results people={data} />}
          {id !== null
            ? !data.some((p) => p.id === id) && <MissingId id={id} />
            : <MgpResults q={q} auto={data.length === 0} />}
        </>
      )}
    </main>
  );
}

function Results({ people }: { people: Person[] }) {
  return (
    <table className="results">
      <thead>
        <tr>
          <th>Name</th>
          <th className="num">Ph.D.</th>
          <th>School</th>
          <th className="num">Students</th>
        </tr>
      </thead>
      <tbody>
        {people.map((p) => (
          <tr key={p.id}>
            <td>
              <Link to={`/m/${p.id}`}>{p.name}</Link>
            </td>
            <td className="num mono">{p.year ?? "—"}</td>
            <td className="muted">
              {p.school ?? "—"}
              {p.country && <span className="small"> · {p.country}</span>}
            </td>
            <td className="num mono">{p.student_count || ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function MissingId({ id }: { id: number }) {
  return (
    <section className="mgp">
      <p className="muted">MGP ID {id} isn’t in the database yet.</p>
      <CrawlButton id={id} lastCrawled={null} label={`Crawl MGP ID ${id}`} />
    </section>
  );
}

/** MGP's own search, run straight away when we have no matches and on request otherwise, since
 * each search is a request to MGP. */
function MgpResults({ q, auto }: { q: string; auto: boolean }) {
  // remembered per query, so the list stays up after crawling someone from it gives us local matches
  const [askedFor, setAskedFor] = useState<string | null>(null);
  useEffect(() => {
    if (auto) setAskedFor(q);
  }, [auto, q]);
  const asked = auto || askedFor === q;
  const mgp = useMgpSearch(q, asked);

  return (
    <section className="mgp">
      <h2 className="label">On the Mathematics Genealogy Project</h2>
      {!asked ? (
        <button type="button" className="btn" onClick={() => setAskedFor(q)}>
          Search MGP for “{q}”
        </button>
      ) : mgp.isPending ? (
        <p className="muted">Searching MGP…</p>
      ) : mgp.isError ? (
        <p className="error">{mgp.error.message}</p>
      ) : (
        <>
          <p className="muted small">
            {mgp.data.length ? `${mgp.data.length} match${mgp.data.length === 1 ? "" : "es"} on MGP.` : "MGP has no one matching."} One word searches
            family names; with more, the first is the given name and the last the family name.
          </p>
          {mgp.data.length > 0 && (
            <table className="results">
              <thead>
                <tr>
                  <th>Name</th>
                  <th className="num">Ph.D.</th>
                  <th>School</th>
                  <th className="num">Crawl</th>
                </tr>
              </thead>
              <tbody>
                {mgp.data.map((h) => (
                  <tr key={h.id}>
                    <td>
                      <Link to={`/m/${h.id}`}>{h.name}</Link>
                      {!h.known && <span className="muted small"> · not in the database</span>}
                    </td>
                    <td className="num mono">{h.year ?? "—"}</td>
                    <td className="muted">{h.school ?? "—"}</td>
                    <td className="num">
                      <CrawlButton id={h.id} lastCrawled={h.last_crawled} label="Crawl" compact />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}
    </section>
  );
}
