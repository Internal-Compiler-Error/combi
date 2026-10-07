import { useEffect } from "react";
import { Link, useSearchParams } from "react-router";
import { useSearch } from "../api/client";

export function SearchPage() {
  const [params] = useSearchParams();
  const q = params.get("q") ?? "";
  const { data, isPending, isError, error } = useSearch(q, 100);
  useEffect(() => void (document.title = `“${q}” · Combi`), [q]);

  return (
    <main className="page narrow">
      <h1 className="page-title">Results for “{q}”</h1>
      {isPending && q && <p className="muted">Searching…</p>}
      {isError && <p className="error">{error.message}</p>}
      {data && (
        <>
          <p className="muted small">{data.length === 100 ? "Showing the first 100 matches." : `${data.length} match${data.length === 1 ? "" : "es"}.`}</p>
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
              {data.map((p) => (
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
        </>
      )}
    </main>
  );
}
